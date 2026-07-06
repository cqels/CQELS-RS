//! Opt-in HTTP transport for the MCP JSON-RPC surface.
//!
//! Java `v2.0.0-alpha.8` added a default-off remote HTTP endpoint for
//! MCP clients. This Rust transport keeps the same deployment posture:
//! stdio remains the default, HTTP binds to loopback by default, and
//! auth / Origin / Host checks are explicit opt-ins.
//!
//! The Origin allow-list is browser defense-in-depth: when configured,
//! requests carrying an `Origin` header must match, while non-browser
//! clients that omit `Origin` are still accepted. Use bearer auth or a
//! fronting proxy for real access control.

use std::{env, io, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{
        header::{AUTHORIZATION, CONTENT_TYPE, HOST, ORIGIN, WWW_AUTHENTICATE},
        HeaderMap, Request, StatusCode,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};

use crate::{
    handle_request, handle_request_with_prompts, handle_request_with_prompts_and_resources,
    handle_request_with_resources, PromptRegistry, ResourceRegistry, ToolRegistry, SERVER_NAME,
};

const DEFAULT_HTTP_HOST: &str = "127.0.0.1";
const DEFAULT_HTTP_PORT: u16 = 3000;
const DEFAULT_HTTP_ENDPOINT: &str = "/mcp";
const DEFAULT_HTTP_HEALTH_PATH: &str = "/health";
const DEFAULT_HTTP_MAX_BODY_BYTES: usize = 1_048_576;

const ENV_TRANSPORT: &str = "CQELS_MCP_TRANSPORT";
const ENV_HTTP_HOST: &str = "CQELS_MCP_HTTP_HOST";
const ENV_HTTP_PORT: &str = "CQELS_MCP_HTTP_PORT";
const ENV_HTTP_PATH: &str = "CQELS_MCP_HTTP_PATH";
const ENV_HTTP_AUTH_TOKEN: &str = "CQELS_MCP_HTTP_AUTH_TOKEN";
const ENV_HTTP_ALLOWED_ORIGINS: &str = "CQELS_MCP_HTTP_ALLOWED_ORIGINS";
const ENV_HTTP_ALLOWED_HOSTS: &str = "CQELS_MCP_HTTP_ALLOWED_HOSTS";
const ENV_HTTP_HEALTH_PATH: &str = "CQELS_MCP_HTTP_HEALTH_PATH";
const ENV_HTTP_MAX_BODY_BYTES: &str = "CQELS_MCP_HTTP_MAX_BODY_BYTES";

/// Transport selected for the MCP server process.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerTransport {
    /// Line-delimited JSON-RPC over stdin/stdout.
    Stdio,
    /// HTTP POST endpoint plus optional health endpoint.
    Http(HttpTransportConfig),
}

/// Configuration for the opt-in HTTP transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpTransportConfig {
    pub host: String,
    pub port: u16,
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_hosts: Vec<String>,
    pub health_path: Option<String>,
    /// `None` disables axum's request-body cap. The default is 1 MiB.
    pub max_body_bytes: Option<usize>,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HTTP_HOST.into(),
            port: DEFAULT_HTTP_PORT,
            endpoint: DEFAULT_HTTP_ENDPOINT.into(),
            auth_token: None,
            allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
            health_path: Some(DEFAULT_HTTP_HEALTH_PATH.into()),
            max_body_bytes: Some(DEFAULT_HTTP_MAX_BODY_BYTES),
        }
    }
}

/// HTTP config parsing / validation failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HttpConfigError {
    #[error("unknown {ENV_TRANSPORT} value (expected 'stdio' or 'http'): {0}")]
    UnknownTransport(String),
    #[error("invalid {name} value '{value}': {message}")]
    InvalidValue {
        name: &'static str,
        value: String,
        message: String,
    },
}

/// Reads the server transport selection from `CQELS_MCP_*` environment
/// variables. Missing or blank `CQELS_MCP_TRANSPORT` defaults to stdio.
pub fn server_transport_from_env() -> Result<ServerTransport, HttpConfigError> {
    server_transport_from_lookup(|key| env::var(key).ok())
}

fn server_transport_from_lookup<F>(lookup: F) -> Result<ServerTransport, HttpConfigError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let transport = lookup(ENV_TRANSPORT).unwrap_or_default();
    let transport = transport.trim();
    if transport.is_empty() || transport.eq_ignore_ascii_case("stdio") {
        return Ok(ServerTransport::Stdio);
    }
    if !transport.eq_ignore_ascii_case("http") {
        return Err(HttpConfigError::UnknownTransport(transport.to_string()));
    }

    Ok(ServerTransport::Http(http_config_from_lookup(lookup)?))
}

fn http_config_from_lookup<F>(lookup: F) -> Result<HttpTransportConfig, HttpConfigError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let mut config = HttpTransportConfig::default();

    if let Some(host) = non_blank(lookup(ENV_HTTP_HOST)) {
        config.host = host;
    }
    if let Some(port) = non_blank(lookup(ENV_HTTP_PORT)) {
        config.port = parse_port(ENV_HTTP_PORT, &port)?;
    }
    if let Some(path) = non_blank(lookup(ENV_HTTP_PATH)) {
        config.endpoint = validate_exact_path(ENV_HTTP_PATH, &path)?;
    }
    if let Some(token) = non_blank(lookup(ENV_HTTP_AUTH_TOKEN)) {
        config.auth_token = Some(token);
    }
    if let Some(origins) = lookup(ENV_HTTP_ALLOWED_ORIGINS) {
        config.allowed_origins = parse_csv(&origins);
    }
    if let Some(hosts) = lookup(ENV_HTTP_ALLOWED_HOSTS) {
        config.allowed_hosts = parse_csv(&hosts);
    }
    if let Some(path) = lookup(ENV_HTTP_HEALTH_PATH) {
        config.health_path = if path.trim().is_empty() {
            None
        } else {
            Some(validate_exact_path(ENV_HTTP_HEALTH_PATH, path.trim())?)
        };
    }
    if let Some(max_body) = non_blank(lookup(ENV_HTTP_MAX_BODY_BYTES)) {
        config.max_body_bytes = parse_body_limit(&max_body)?;
    }

    if config.health_path.as_deref() == Some(config.endpoint.as_str()) {
        return Err(HttpConfigError::InvalidValue {
            name: ENV_HTTP_HEALTH_PATH,
            value: config.endpoint.clone(),
            message: "must differ from CQELS_MCP_HTTP_PATH".into(),
        });
    }

    Ok(config)
}

fn non_blank(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_port(name: &'static str, value: &str) -> Result<u16, HttpConfigError> {
    value
        .parse::<u16>()
        .map_err(|e| HttpConfigError::InvalidValue {
            name,
            value: value.into(),
            message: format!("must be an integer in [0, 65535]: {e}"),
        })
}

fn parse_body_limit(value: &str) -> Result<Option<usize>, HttpConfigError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|e| HttpConfigError::InvalidValue {
            name: ENV_HTTP_MAX_BODY_BYTES,
            value: value.into(),
            message: format!("must be an integer number of bytes; <= 0 disables: {e}"),
        })?;
    if parsed <= 0 {
        Ok(None)
    } else {
        Ok(Some(parsed as usize))
    }
}

fn validate_exact_path(name: &'static str, value: &str) -> Result<String, HttpConfigError> {
    let valid_chars = value.bytes().all(|b| {
        matches!(
            b,
            b'/' | b'-' | b'.' | b'_' | b'~' | b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z'
        )
    });
    if !value.starts_with('/') || value == "/" || value.ends_with('/') || !valid_chars {
        return Err(HttpConfigError::InvalidValue {
            name,
            value: value.into(),
            message: "must be an exact path: starts with '/', is not '/', has no trailing '/', and uses only URL-path-safe ASCII characters".into(),
        });
    }
    Ok(value.into())
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Runs the HTTP transport until the axum server returns.
pub async fn run_http(registry: Arc<ToolRegistry>, config: HttpTransportConfig) -> io::Result<()> {
    let app = http_router(registry, config.clone());
    let listener = tokio::net::TcpListener::bind((config.host.as_str(), config.port)).await?;
    let addr = listener.local_addr()?;
    eprintln!(
        "{SERVER_NAME}: HTTP MCP transport listening on http://{addr}{}",
        config.endpoint
    );
    axum::serve(listener, app).await
}

/// Runs the HTTP transport with tools, prompts, and resources enabled.
pub async fn run_http_with_prompts_and_resources(
    registry: Arc<ToolRegistry>,
    prompts: Arc<PromptRegistry>,
    resources: Arc<ResourceRegistry>,
    config: HttpTransportConfig,
) -> io::Result<()> {
    let app = http_router_with_prompts_and_resources(registry, prompts, resources, config.clone());
    let listener = tokio::net::TcpListener::bind((config.host.as_str(), config.port)).await?;
    let addr = listener.local_addr()?;
    eprintln!(
        "{SERVER_NAME}: HTTP MCP transport listening on http://{addr}{}",
        config.endpoint
    );
    axum::serve(listener, app).await
}

#[derive(Clone)]
struct HttpState {
    registry: Arc<ToolRegistry>,
    prompts: Option<Arc<PromptRegistry>>,
    resources: Option<Arc<ResourceRegistry>>,
    config: HttpTransportConfig,
}

fn http_router(registry: Arc<ToolRegistry>, config: HttpTransportConfig) -> Router {
    http_router_inner(registry, None, None, config)
}

fn http_router_with_prompts_and_resources(
    registry: Arc<ToolRegistry>,
    prompts: Arc<PromptRegistry>,
    resources: Arc<ResourceRegistry>,
    config: HttpTransportConfig,
) -> Router {
    http_router_inner(registry, Some(prompts), Some(resources), config)
}

fn http_router_inner(
    registry: Arc<ToolRegistry>,
    prompts: Option<Arc<PromptRegistry>>,
    resources: Option<Arc<ResourceRegistry>>,
    config: HttpTransportConfig,
) -> Router {
    let state = Arc::new(HttpState {
        registry,
        prompts,
        resources,
        config: config.clone(),
    });

    let mcp_route = post(handle_mcp_post)
        .layer(if let Some(limit) = config.max_body_bytes {
            DefaultBodyLimit::max(limit)
        } else {
            DefaultBodyLimit::disable()
        })
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_http_guards,
        ));

    let mut router = Router::new().route(&config.endpoint, mcp_route);
    if let Some(path) = &config.health_path {
        router = router.route(path, get(health));
    }

    router.with_state(state)
}

async fn enforce_http_guards(
    State(state): State<Arc<HttpState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if let Some(response) = guard_failure(&state.config, request.headers()) {
        return response;
    }
    next.run(request).await
}

fn guard_failure(config: &HttpTransportConfig, headers: &HeaderMap) -> Option<Response> {
    if let Some(expected) = &config.auth_token {
        let presented = bearer_token(headers);
        let ok = presented
            .as_deref()
            .is_some_and(|token| constant_time_eq(expected.as_bytes(), token.as_bytes()));
        if !ok {
            return Some(json_error(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                Some((WWW_AUTHENTICATE.as_str(), "Bearer")),
            ));
        }
    }

    if !config.allowed_origins.is_empty() {
        if let Some(origin) = header_str(headers, ORIGIN.as_str()) {
            if !config
                .allowed_origins
                .iter()
                .any(|allowed| allowed == origin)
            {
                return Some(json_error(
                    StatusCode::FORBIDDEN,
                    "origin not allowed",
                    None,
                ));
            }
        }
    }

    if !config.allowed_hosts.is_empty() {
        let host_allowed = header_str(headers, HOST.as_str())
            .is_some_and(|host| config.allowed_hosts.iter().any(|allowed| allowed == host));
        if !host_allowed {
            return Some(json_error(StatusCode::FORBIDDEN, "host not allowed", None));
        }
    }

    None
}

async fn handle_mcp_post(State(state): State<Arc<HttpState>>, body: Bytes) -> Response {
    let request = match std::str::from_utf8(&body) {
        Ok(body) => body.trim().to_string(),
        Err(_) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "request body must be valid UTF-8 JSON-RPC",
                None,
            );
        }
    };
    if request.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "request body is empty", None);
    }

    let registry = state.registry.clone();
    let prompts = state.prompts.clone();
    let resources = state.resources.clone();
    let dispatch =
        tokio::task::spawn_blocking(move || match (prompts.as_deref(), resources.as_deref()) {
            (Some(prompts), Some(resources)) => {
                handle_request_with_prompts_and_resources(&registry, prompts, resources, &request)
            }
            (Some(prompts), None) => handle_request_with_prompts(&registry, prompts, &request),
            (None, Some(resources)) => {
                handle_request_with_resources(&registry, resources, &request)
            }
            (None, None) => handle_request(&registry, &request),
        })
        .await;
    let dispatch = match dispatch {
        Ok(response) => response,
        Err(e) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("JSON-RPC dispatch task failed: {e}"),
                None,
            );
        }
    };

    match dispatch {
        Some(response) => (
            StatusCode::OK,
            [(CONTENT_TYPE.as_str(), "application/json")],
            response,
        )
            .into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE.as_str(), "application/json")],
        r#"{"status":"ok"}"#,
    )
}

fn json_error(
    status: StatusCode,
    message: &str,
    extra_header: Option<(&'static str, &'static str)>,
) -> Response {
    let body = serde_json::json!({ "error": message }).to_string();
    let mut response =
        (status, [(CONTENT_TYPE.as_str(), "application/json")], body).into_response();
    if let Some((name, value)) = extra_header {
        response
            .headers_mut()
            .insert(name, value.parse().expect("valid static header value"));
    }
    response
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok().map(str::trim)
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = header_str(headers, AUTHORIZATION.as_str())?;
    if value.len() <= "Bearer ".len() || !value[.."Bearer ".len()].eq_ignore_ascii_case("Bearer ") {
        return None;
    }
    let token = value["Bearer ".len()..].trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // Content-constant comparison for bearer secrets. Authorization
    // already reveals the presented token length, so length is not
    // treated as secret here.
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for idx in 0..max_len {
        let l = left.get(idx).copied().unwrap_or(0);
        let r = right.get(idx).copied().unwrap_or(0);
        diff |= usize::from(l ^ r);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body;
    use serde_json::Value as JsonValue;
    use tower::ServiceExt;

    use crate::{
        cqels_prompt_registry, cqels_resource_registry, parse_query_tool, query_tool, reason_tool,
        reasoning_profiles_tool, shacl_capabilities_tool, store_memory_tool, InMemoryMemoryStore,
        MemoryStore,
    };

    fn lookup_from<'a>(
        pairs: &'a [(&'static str, &'a str)],
    ) -> impl Fn(&'static str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find_map(|(k, v)| (*k == key).then(|| (*v).to_string()))
        }
    }

    fn registry() -> Arc<ToolRegistry> {
        let mut reg = ToolRegistry::new();
        reg.install(parse_query_tool());
        reg.install(query_tool());
        reg.install(reasoning_profiles_tool());
        reg.install(shacl_capabilities_tool());
        reg.install(reason_tool());
        Arc::new(reg)
    }

    fn registry_with_memory() -> Arc<ToolRegistry> {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let mut reg = ToolRegistry::new();
        reg.install(store_memory_tool(memory));
        Arc::new(reg)
    }

    async fn response_body(response: Response) -> String {
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        String::from_utf8(bytes.to_vec()).expect("utf8 response")
    }

    #[test]
    fn stdio_is_default_transport() {
        let selected = server_transport_from_lookup(lookup_from(&[])).expect("transport");
        assert_eq!(selected, ServerTransport::Stdio);
    }

    #[test]
    fn parses_http_transport_defaults_and_overrides() {
        let selected = server_transport_from_lookup(lookup_from(&[
            (ENV_TRANSPORT, "http"),
            (ENV_HTTP_HOST, "0.0.0.0"),
            (ENV_HTTP_PORT, "3911"),
            (ENV_HTTP_PATH, "/agent/mcp"),
            (ENV_HTTP_AUTH_TOKEN, "secret"),
            (
                ENV_HTTP_ALLOWED_ORIGINS,
                "https://chatgpt.com, https://example.org ",
            ),
            (ENV_HTTP_ALLOWED_HOSTS, "mcp.example.org, localhost:3911"),
            (ENV_HTTP_HEALTH_PATH, "/ready"),
            (ENV_HTTP_MAX_BODY_BYTES, "2048"),
        ]))
        .expect("transport");

        let ServerTransport::Http(config) = selected else {
            panic!("expected http transport");
        };
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 3911);
        assert_eq!(config.endpoint, "/agent/mcp");
        assert_eq!(config.auth_token.as_deref(), Some("secret"));
        assert_eq!(
            config.allowed_origins,
            vec!["https://chatgpt.com", "https://example.org"]
        );
        assert_eq!(
            config.allowed_hosts,
            vec!["mcp.example.org", "localhost:3911"]
        );
        assert_eq!(config.health_path.as_deref(), Some("/ready"));
        assert_eq!(config.max_body_bytes, Some(2048));
    }

    #[test]
    fn rejects_non_exact_http_paths() {
        for bad_path in ["/mcp/", "/mcp?x=1", "/mcp#frag", "/mcp space", "mcp"] {
            let err = server_transport_from_lookup(lookup_from(&[
                (ENV_TRANSPORT, "http"),
                (ENV_HTTP_PATH, bad_path),
            ]))
            .expect_err("bad path should be rejected");
            assert!(
                err.to_string().contains("exact path"),
                "unexpected error for {bad_path:?}: {err}"
            );
        }
    }

    #[test]
    fn max_body_zero_disables_limit() {
        let selected = server_transport_from_lookup(lookup_from(&[
            (ENV_TRANSPORT, "http"),
            (ENV_HTTP_MAX_BODY_BYTES, "0"),
        ]))
        .expect("transport");
        let ServerTransport::Http(config) = selected else {
            panic!("expected http transport");
        };
        assert_eq!(config.max_body_bytes, None);
    }

    #[tokio::test]
    async fn http_post_dispatches_json_rpc() {
        let app = http_router(registry(), HttpTransportConfig::default());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(HOST, "127.0.0.1:3000")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        let json: JsonValue = serde_json::from_str(&body).expect("json response");
        assert_eq!(json["id"], 1);
        assert!(json["result"].is_object());
    }

    #[tokio::test]
    async fn http_post_advertises_prompts_and_resources_when_configured() {
        let app = http_router_with_prompts_and_resources(
            registry(),
            Arc::new(cqels_prompt_registry()),
            Arc::new(cqels_resource_registry()),
            HttpTransportConfig::default(),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        let json: JsonValue = serde_json::from_str(&body).expect("json response");
        let capabilities = &json["result"]["capabilities"];
        assert!(capabilities["tools"].is_object());
        assert!(capabilities["prompts"].is_object());
        assert!(capabilities["resources"].is_object());
    }

    #[tokio::test]
    async fn http_post_dispatches_stateful_tool_on_blocking_pool() {
        let app = http_router(registry_with_memory(), HttpTransportConfig::default());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::from(
                        serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "tools/call",
                            "params": {
                                "name": "store_memory",
                                "arguments": {
                                    "id": "alpha8-http",
                                    "content": "HTTP transport stores memory"
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        let json: JsonValue = serde_json::from_str(&body).expect("json response");
        assert_eq!(json["id"], 2);
        assert_eq!(json["result"]["isError"], false);
        assert_eq!(json["result"]["content"]["id"], "alpha8-http");
    }

    #[tokio::test]
    async fn http_notification_returns_accepted_without_body() {
        let app = http_router(registry(), HttpTransportConfig::default());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::from(r#"{"jsonrpc":"2.0","method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response_body(response).await.is_empty());
    }

    #[tokio::test]
    async fn health_endpoint_is_unauthenticated() {
        let config = HttpTransportConfig {
            auth_token: Some("secret".into()),
            ..HttpTransportConfig::default()
        };
        let app = http_router(registry(), config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body(response).await, r#"{"status":"ok"}"#);
    }

    #[tokio::test]
    async fn bearer_auth_rejects_missing_or_wrong_token() {
        let config = HttpTransportConfig {
            auth_token: Some("secret".into()),
            ..HttpTransportConfig::default()
        };
        let app = http_router(registry(), config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[WWW_AUTHENTICATE.as_str()],
            "Bearer",
            "401 should advertise the bearer challenge"
        );
    }

    #[tokio::test]
    async fn bearer_auth_accepts_configured_token() {
        let config = HttpTransportConfig {
            auth_token: Some("secret".into()),
            ..HttpTransportConfig::default()
        };
        let app = http_router(registry(), config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn origin_and_host_allow_lists_are_enforced() {
        let config = HttpTransportConfig {
            allowed_origins: vec!["https://chatgpt.com".into()],
            allowed_hosts: vec!["mcp.example.org".into()],
            ..HttpTransportConfig::default()
        };
        let app = http_router(registry(), config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header(ORIGIN, "https://evil.example")
                    .header(HOST, "mcp.example.org")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn body_limit_rejects_oversized_json_rpc_before_dispatch() {
        let config = HttpTransportConfig {
            max_body_bytes: Some(8),
            ..HttpTransportConfig::default()
        };
        let app = http_router(registry(), config);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .expect("route response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
