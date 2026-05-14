//! [`IotDbExecutor`] backed by the official `iotdb-client-rs` Thrift
//! session client.
//!
//! Only compiled when the `thrift` feature is enabled. The default
//! build keeps `iotdb-client-rs` and its Thrift transitive deps out
//! of the dependency graph entirely.
//!
//! ## Why a dedicated worker thread?
//!
//! `iotdb-client-rs::RpcSession` is a synchronous `&mut self` client
//! whose internal Thrift protocol handles
//! (`dyn TInputProtocol` / `dyn TOutputProtocol`) carry no
//! `Send + Sync` bounds. That makes the session itself **`!Send`**:
//! it can't be moved across threads, can't be wrapped in
//! `Arc<Mutex<_>>` for shared access (a `Mutex<!Send>` is not `Sync`),
//! and can't be parked behind `tokio::task::spawn_blocking` (which
//! requires the captured closure to be `Send`).
//!
//! The standard workaround is to pin the session to one dedicated
//! OS thread and talk to it through channels. This module spins up
//! that thread in [`ThriftIotDbExecutor::connect`] and exposes an
//! async surface that forwards every method call as a
//! `SessionCommand` over a [`tokio::sync::mpsc`] channel, awaiting a
//! one-shot reply. The session lives on the worker thread for its
//! whole lifetime; the executor itself is `Send + Sync` because all
//! it carries is the `mpsc::Sender` half.
//!
//! ## Payload encoding
//!
//! IoTDB has no binary measurement variant in `iotdb-client-rs`
//! v0.3.x's `Value` enum (only `Bool`, `Int32`, `Int64`, `Float`,
//! `Double`, `Text`). Payload bytes round-trip as base64 in a single
//! `payload` TEXT measurement per device.
//!
//! ## Live integration tests
//!
//! Real-server tests are gated by the `IOTDB_HOST` env var so they
//! stay opt-in. With a server running:
//!
//! ```bash
//! IOTDB_HOST=localhost cargo test -p cqels-storage-iotdb \
//!   --features thrift -- --ignored thrift_live
//! ```

use std::thread;

use async_trait::async_trait;
use base64::Engine;
use iotdb::client::remote::{Config as IotDbClientConfig, RpcSession};
use iotdb::client::{DataSet, Session, Value};
use tokio::sync::{mpsc, oneshot};

use crate::executor::{IotDbError, IotDbExecutor};

/// Single measurement name stored per device.
const PAYLOAD_MEASUREMENT: &str = "payload";

/// Connection parameters for a real IoTDB Thrift session.
///
/// Mirrors the fields of [`iotdb::client::remote::Config`] but lives
/// in this crate so callers don't have to depend on the iotdb crate
/// directly.
#[derive(Clone, Debug)]
pub struct ThriftConnectConfig {
    pub host: String,
    pub port: i32,
    pub user: String,
    pub password: String,
    /// Time-zone string (e.g. `"UTC+8"`). IoTDB requires one; defaults
    /// to `"UTC+0"` when you don't care.
    pub time_zone: String,
    /// Fetch size for query result streams. Defaults to 1024.
    pub fetch_size: i32,
}

impl ThriftConnectConfig {
    /// Constructs a config pointing at `host:port` with the supplied
    /// credentials and defaults for the rest.
    pub fn new(
        host: impl Into<String>,
        port: i32,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            time_zone: "UTC+0".to_string(),
            fetch_size: 1024,
        }
    }
}

/// [`IotDbExecutor`] backed by a real Thrift session running on a
/// dedicated worker thread.
///
/// Cloneable handle around the worker — drop the last instance to
/// drain the channel and let the worker shut down.
#[derive(Clone)]
pub struct ThriftIotDbExecutor {
    sender: mpsc::Sender<SessionCommand>,
}

impl ThriftIotDbExecutor {
    /// Opens a Thrift session and pins it to a worker thread.
    /// Returns once the worker has confirmed the session opened
    /// successfully.
    pub fn connect(config: ThriftConnectConfig) -> Result<Self, IotDbError> {
        // Buffered enough to absorb short bursts without parking the
        // sending task. Tunable later if it ever shows up in profiles.
        let (tx, rx) = mpsc::channel::<SessionCommand>(64);
        let (open_tx, open_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        thread::Builder::new()
            .name("cqels-iotdb-worker".into())
            .spawn(move || run_worker(config, rx, open_tx))
            .map_err(|e| IotDbError::Backend(format!("spawn worker thread: {e}")))?;

        // Wait for the worker to either confirm `session.open()`
        // succeeded or report the failure. Doing this synchronously
        // means a failed connection surfaces from `connect` itself
        // rather than from the first downstream call.
        match open_rx.recv() {
            Ok(Ok(())) => Ok(Self { sender: tx }),
            Ok(Err(e)) => Err(IotDbError::Backend(e)),
            Err(e) => Err(IotDbError::Backend(format!(
                "worker thread died before opening session: {e}"
            ))),
        }
    }

    async fn dispatch<R, F>(&self, build: F) -> Result<R, IotDbError>
    where
        F: FnOnce(oneshot::Sender<Result<R, String>>) -> SessionCommand,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(build(reply_tx))
            .await
            .map_err(|_| IotDbError::Backend("iotdb worker thread closed".into()))?;
        reply_rx
            .await
            .map_err(|_| IotDbError::Backend("iotdb worker dropped reply channel".into()))?
            .map_err(IotDbError::Backend)
    }
}

#[async_trait]
impl IotDbExecutor for ThriftIotDbExecutor {
    async fn put(&self, device: &str, key: u64, data: Vec<u8>) -> Result<(), IotDbError> {
        let device = device.to_string();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
        self.dispatch(move |reply| SessionCommand::Put {
            device,
            key,
            encoded,
            reply,
        })
        .await
    }

    async fn read_from(
        &self,
        device: &str,
        start_inclusive: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IotDbError> {
        let device = device.to_string();
        self.dispatch(move |reply| SessionCommand::ReadFrom {
            device,
            start_inclusive,
            reply,
        })
        .await
    }

    async fn latest(&self, device: &str) -> Result<Option<(u64, Vec<u8>)>, IotDbError> {
        let device = device.to_string();
        self.dispatch(move |reply| SessionCommand::Latest { device, reply })
            .await
    }

    async fn delete_range(
        &self,
        device: &str,
        start_inclusive: u64,
        end_exclusive: u64,
    ) -> Result<(), IotDbError> {
        if start_inclusive >= end_exclusive {
            return Ok(());
        }
        let device = device.to_string();
        self.dispatch(move |reply| SessionCommand::DeleteRange {
            device,
            start_inclusive,
            end_exclusive,
            reply,
        })
        .await
    }

    async fn close(&self) -> Result<(), IotDbError> {
        self.dispatch(|reply| SessionCommand::Close { reply }).await
    }
}

/// One row in the IoTDB time-series: `(time, payload_bytes)`.
type Row = (u64, Vec<u8>);

/// One-shot reply channel for a worker command, parameterized by the
/// success type. Errors always flatten to `String` before crossing
/// the channel because the underlying iotdb-client-rs errors aren't
/// `Send`.
type Reply<T> = oneshot::Sender<Result<T, String>>;

/// Messages handled by the dedicated worker thread.
enum SessionCommand {
    Put {
        device: String,
        key: u64,
        encoded: String,
        reply: Reply<()>,
    },
    ReadFrom {
        device: String,
        start_inclusive: u64,
        reply: Reply<Vec<Row>>,
    },
    Latest {
        device: String,
        reply: Reply<Option<Row>>,
    },
    DeleteRange {
        device: String,
        start_inclusive: u64,
        end_exclusive: u64,
        reply: Reply<()>,
    },
    Close {
        reply: Reply<()>,
    },
}

fn run_worker(
    config: ThriftConnectConfig,
    mut rx: mpsc::Receiver<SessionCommand>,
    open_reply: std::sync::mpsc::Sender<Result<(), String>>,
) {
    // `TSProtocolVersion` lives in a private module so we can't name
    // it directly; rely on the `TypedBuilder` default. The other
    // tunables all map directly.
    let client_config = IotDbClientConfig::builder()
        .host(config.host)
        .port(config.port)
        .username(config.user)
        .password(config.password)
        .timezone(Some(config.time_zone))
        .fetch_size(config.fetch_size)
        .build();
    let mut session = match RpcSession::new(client_config) {
        Ok(s) => s,
        Err(e) => {
            let _ = open_reply.send(Err(format!("RpcSession::new failed: {e}")));
            return;
        }
    };
    if let Err(e) = session.open() {
        let _ = open_reply.send(Err(format!("session.open failed: {e}")));
        return;
    }
    if open_reply.send(Ok(())).is_err() {
        // Connect() caller hung up before we could confirm; just
        // close and exit cleanly.
        let _ = session.close();
        return;
    }

    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            SessionCommand::Put {
                device,
                key,
                encoded,
                reply,
            } => {
                let result = session
                    .insert_record(
                        &device,
                        vec![PAYLOAD_MEASUREMENT],
                        vec![Value::Text(encoded)],
                        key as i64,
                        None,
                    )
                    .map_err(|e| format!("insert_record failed: {e}"));
                let _ = reply.send(result);
            }
            SessionCommand::ReadFrom {
                device,
                start_inclusive,
                reply,
            } => {
                let sql = format!(
                    "select {PAYLOAD_MEASUREMENT} from {device} where time >= {start_inclusive}"
                );
                let result = match session.execute_query_statement(&sql, None) {
                    Ok(ds) => decode_dataset(ds),
                    Err(e) => Err(format!("execute_query_statement failed: {e}")),
                };
                let _ = reply.send(result);
            }
            SessionCommand::Latest { device, reply } => {
                // IoTDB's natural row order on `SELECT` is ascending
                // by time, so the simplest portable implementation
                // is "drain and take the last row". O(n) — fine for
                // the scaffold semantic of "checkpoint store
                // latest" where n is small.
                let sql = format!("select {PAYLOAD_MEASUREMENT} from {device}");
                let result = match session.execute_query_statement(&sql, None) {
                    Ok(ds) => decode_dataset(ds).map(|rows| rows.into_iter().last()),
                    Err(e) => Err(format!("execute_query_statement failed: {e}")),
                };
                let _ = reply.send(result);
            }
            SessionCommand::DeleteRange {
                device,
                start_inclusive,
                end_exclusive,
                reply,
            } => {
                // IoTDB's `delete_data` is inclusive on both ends; the
                // SPI uses half-open `[start, end)` so we delete
                // `[start, end - 1]`.
                let path = format!("{device}.{PAYLOAD_MEASUREMENT}");
                let result = session
                    .delete_data(
                        vec![path.as_str()],
                        start_inclusive as i64,
                        end_exclusive.saturating_sub(1) as i64,
                    )
                    .map_err(|e| format!("delete_data failed: {e}"));
                let _ = reply.send(result);
            }
            SessionCommand::Close { reply } => {
                let result = session
                    .close()
                    .map_err(|e| format!("session.close failed: {e}"));
                let _ = reply.send(result);
                return;
            }
        }
    }
    // Channel closed by the last sender drop. Close the session
    // best-effort so we don't leak it on the server side.
    let _ = session.close();
}

/// Drains a `DataSet` of `(time, payload-as-text)` rows, decoding
/// base64 payloads back to `Vec<u8>` and tagging each with its IoTDB
/// timestamp.
fn decode_dataset(dataset: Box<dyn DataSet + '_>) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    for record in dataset {
        let timestamp = record.timestamp;
        let payload_value = record
            .values
            .into_iter()
            .next()
            .ok_or_else(|| "row missing payload column".to_string())?;
        let encoded = match payload_value {
            Value::Text(s) => s,
            other => {
                return Err(format!("expected Text payload column, got {other:?}"));
            }
        };
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .map_err(|e| format!("base64 decode failed: {e}"))?;
        if timestamp < 0 {
            return Err(format!(
                "negative IoTDB timestamp not representable as u64: {timestamp}"
            ));
        }
        rows.push((timestamp as u64, bytes));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IotDbExecutor, IotDbPersistentBackend};
    use std::sync::Arc as StdArc;

    /// Compile-only smoke test: the executor handle is `Send + Sync`
    /// and dyn-objectifiable through the SPI's
    /// `Arc<dyn IotDbExecutor>` slot. Connection itself isn't
    /// exercised here — that lives in `thrift_live_round_trip`.
    #[test]
    fn type_plumbing_compiles_and_objectifies() {
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<ThriftIotDbExecutor>();
        // dyn-objectifiable through the SPI:
        fn _to_dyn(exec: ThriftIotDbExecutor) -> StdArc<dyn IotDbExecutor> {
            StdArc::new(exec)
        }
        // and the persistent backend accepts that dyn handle:
        fn _build_backend(
            arc: StdArc<dyn IotDbExecutor>,
        ) -> Result<IotDbPersistentBackend, cqels_storage_spi::StorageError> {
            IotDbPersistentBackend::with_executor(arc)
        }
        let _ = _to_dyn;
        let _ = _build_backend;
    }

    /// Verifies `connect` surfaces a clean error rather than panicking
    /// when no IoTDB cluster is reachable.
    #[test]
    fn connect_to_dead_endpoint_yields_clean_error() {
        // Port 1 is reserved and almost always closed.
        let config = ThriftConnectConfig::new("127.0.0.1", 1, "root", "root");
        let result = ThriftIotDbExecutor::connect(config);
        assert!(matches!(result, Err(IotDbError::Backend(_))));
    }

    /// Live round-trip against a real IoTDB cluster. Skipped unless
    /// `IOTDB_HOST` is set; ignored by default so it never fires in
    /// regular test runs.
    #[tokio::test]
    #[ignore = "requires a running IoTDB cluster reachable at $IOTDB_HOST"]
    async fn thrift_live_round_trip() {
        use cqels_storage_spi::{CheckpointManifest, PersistentBackend, StreamEnvelope};

        let Ok(host) = std::env::var("IOTDB_HOST") else {
            eprintln!("IOTDB_HOST not set — skipping live test");
            return;
        };
        let port: i32 = std::env::var("IOTDB_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6667);
        let user = std::env::var("IOTDB_USER").unwrap_or_else(|_| "root".into());
        let password = std::env::var("IOTDB_PASSWORD").unwrap_or_else(|_| "root".into());

        let config = ThriftConnectConfig::new(host, port, user, password);
        let executor = ThriftIotDbExecutor::connect(config).expect("thrift connect");
        let executor: StdArc<dyn IotDbExecutor> = StdArc::new(executor);

        // Use a test-local device path so we don't trample real data.
        let backend = IotDbPersistentBackend::with_paths(
            executor,
            "root.cqels_test.journal".into(),
            "root.cqels_test.checkpoints".into(),
        )
        .expect("backend");

        // Round-trip an envelope.
        let env = StreamEnvelope::new(0, "test", 0, "text/plain", b"hello".to_vec());
        let offset = backend.event_journal().append(env).await.expect("append");
        assert!(offset >= 1);

        let read_back = backend
            .event_journal()
            .read_from(offset - 1)
            .await
            .expect("read_from");
        assert!(
            read_back.iter().any(|e| e.payload == b"hello"),
            "live IoTDB returned: {:?}",
            read_back
        );

        // Round-trip a checkpoint.
        backend
            .checkpoint_store()
            .write_checkpoint(
                CheckpointManifest::new(42, 1_700_000_000, offset),
                std::collections::HashMap::new(),
            )
            .await
            .expect("write_checkpoint");
        let latest = backend
            .checkpoint_store()
            .latest()
            .await
            .expect("latest")
            .expect("some checkpoint");
        assert_eq!(latest.manifest.id, 42);

        // Clean up.
        backend
            .event_journal()
            .truncate_before(offset)
            .await
            .expect("truncate");
        backend
            .checkpoint_store()
            .delete_older_than(100)
            .await
            .expect("delete_older_than");
    }
}
