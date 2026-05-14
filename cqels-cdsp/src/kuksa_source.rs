//! [`VssSignalSource`] backed by the official `kuksa-rust-sdk` gRPC
//! Databroker v2 client.
//!
//! Only compiled when the `kuksa` feature is enabled. The default
//! build keeps `kuksa-rust-sdk` (and its tonic / prost / `protoc`
//! build-time chain) out of the dependency graph entirely.
//!
//! ## Wire pattern
//!
//! Unlike the IoTDB Thrift client in `cqels-storage-iotdb` — whose
//! session is `!Send` and needs a dedicated worker thread —
//! kuksa-rust-sdk is built on tonic and is fully async, `Send + Sync`
//! out of the box. The integration is a single background tokio task:
//!
//! 1. [`KuksaVssSignalSource::connect`] opens a [`KuksaClientV2`],
//!    calls `subscribe(paths, …)` to validate the subscription
//!    synchronously (so bad endpoints or unknown paths surface from
//!    `connect` itself, not from the first `next()`), and gets back
//!    a `tonic::Streaming<SubscribeResponse>`.
//! 2. A spawned task drains the stream. Each `SubscribeResponse`
//!    carries a `map<string, Datapoint>`; the task converts every
//!    `(path, Datapoint)` into a [`VssSignal`] and pushes it into a
//!    bounded [`tokio::sync::mpsc`] channel.
//! 3. The source's [`VssSignalSource::next`] impl just `recv()`s
//!    from the channel.
//!
//! `mpsc::channel` back-pressures the gRPC drain task when downstream
//! consumers fall behind: `send` awaits free capacity, so the network
//! stream stalls instead of growing an unbounded queue. The capacity
//! is configurable via [`KuksaConnectConfig`].
//!
//! ## VSS value mapping
//!
//! kuksa-rust-sdk's `value::TypedValue` covers more variants than
//! cqels-cdsp's narrow [`VssValue`] enum (scalar bool / i64 / f64 /
//! string). The conversion:
//!
//! - `String` / `Bool` map verbatim.
//! - Signed integers (`Int32`, `Int64`) lift to `VssValue::Int(i64)`.
//! - Unsigned `Uint32` lifts to `VssValue::Int(i64)`; `Uint64` lifts
//!   only when it fits in `i64`, otherwise the signal is dropped with
//!   a warning.
//! - Floats (`Float`, `Double`) lift to `VssValue::Float(f64)`.
//! - Array variants are dropped with a warning — VSS array signals
//!   are uncommon in stream-processing scenarios and require a richer
//!   `VssValue` enum that's out of scope for this crate's scaffold.
//!
//! Dropped values are logged via `tracing::warn!` when the crate is
//! built with `tracing`, but this crate doesn't pull in tracing
//! directly — failures are silent today and can be wired into a
//! follow-up if observability matters.
//!
//! ## Live integration tests
//!
//! Real-server tests are gated by `KUKSA_HOST` so they stay opt-in.
//! With a Databroker running:
//!
//! ```bash
//! KUKSA_HOST=http://localhost:55555 cargo test -p cqels-cdsp \
//!   --features kuksa -- --ignored kuksa_live
//! ```

use std::time::SystemTime;

use async_trait::async_trait;
use http::Uri;
use kuksa_rust_sdk::kuksa::common::types::SubscribeResponseTypeV2;
use kuksa_rust_sdk::kuksa::common::ClientTraitV2;
use kuksa_rust_sdk::kuksa::val::v2::KuksaClientV2;
use kuksa_rust_sdk::v2_proto::{value::TypedValue, Datapoint, Value};
use tokio::sync::mpsc;

use crate::error::CdspError;
use crate::signal::{VssSignal, VssValue};
use crate::source::VssSignalSource;

/// Connection parameters for a KUKSA.val Databroker subscription.
#[derive(Clone, Debug)]
pub struct KuksaConnectConfig {
    /// Databroker URI, e.g. `"http://localhost:55555"`. TLS variants
    /// require enabling the upstream SDK's `tls` feature.
    pub uri: String,
    /// VSS signal paths to subscribe to, e.g.
    /// `["Vehicle.Speed", "Vehicle.Powertrain.FuelSystem.Level"]`.
    pub paths: Vec<String>,
    /// Channel buffer between the gRPC drain task and the source's
    /// [`VssSignalSource::next`] consumer. Defaults to 256.
    pub buffer_size: usize,
    /// Optional KUKSA-side server buffer override. If the subscriber
    /// is slow, Databroker buffers up to this many messages before
    /// dropping oldest first. Defaults to None (server default).
    pub server_buffer_size: Option<u32>,
    /// Optional minimum sample interval in milliseconds, applied
    /// server-side. Defaults to None (every change).
    pub min_sample_interval_ms: Option<u32>,
}

impl KuksaConnectConfig {
    /// Constructs a config pointing at `uri` and subscribing to the
    /// supplied paths, with default channel + interval settings.
    pub fn new(uri: impl Into<String>, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            uri: uri.into(),
            paths: paths.into_iter().map(Into::into).collect(),
            buffer_size: 256,
            server_buffer_size: None,
            min_sample_interval_ms: None,
        }
    }
}

/// [`VssSignalSource`] backed by a live KUKSA.val Databroker v2
/// gRPC subscription.
///
/// Cheap to clone: holds an `Arc`-equivalent over the channel
/// receiver via [`tokio::sync::Mutex`]. The background drain task
/// keeps running until either the upstream stream ends or all
/// receiver handles drop.
pub struct KuksaVssSignalSource {
    receiver: tokio::sync::Mutex<mpsc::Receiver<VssSignal>>,
}

impl KuksaVssSignalSource {
    /// Opens a Databroker subscription matching `config` and returns
    /// the source. The subscription is validated synchronously:
    /// connection failures and unknown paths surface from `connect`
    /// itself rather than from the first downstream `next()`.
    pub async fn connect(config: KuksaConnectConfig) -> Result<Self, CdspError> {
        let uri: Uri = config.uri.parse().map_err(|e: http::uri::InvalidUri| {
            CdspError::InvalidValue(format!("invalid KUKSA URI `{}`: {e}", config.uri))
        })?;
        let mut client = KuksaClientV2::new(uri);

        let stream = client
            .subscribe(
                config.paths.clone(),
                config.server_buffer_size,
                config.min_sample_interval_ms,
            )
            .await
            .map_err(|e| CdspError::InvalidValue(format!("kuksa subscribe failed: {e:?}")))?;

        let (tx, rx) = mpsc::channel::<VssSignal>(config.buffer_size);

        // Move the client into the drain task too so the channel
        // stays open for the stream's lifetime. The client's only
        // role from here on is to keep the connection alive; the
        // stream borrows nothing further from it.
        tokio::spawn(drain_subscription(client, stream, tx));

        Ok(Self {
            receiver: tokio::sync::Mutex::new(rx),
        })
    }
}

#[async_trait]
impl VssSignalSource for KuksaVssSignalSource {
    async fn next(&self) -> Option<VssSignal> {
        let mut guard = self.receiver.lock().await;
        guard.recv().await
    }
}

/// Long-lived background task that drains the gRPC subscription
/// stream into the source's mpsc channel. The `_client` parameter is
/// only held to keep the gRPC connection alive for the stream's
/// lifetime; we never call into it again.
async fn drain_subscription(
    _client: KuksaClientV2,
    mut stream: SubscribeResponseTypeV2,
    tx: mpsc::Sender<VssSignal>,
) {
    loop {
        let message = match stream.message().await {
            Ok(Some(msg)) => msg,
            Ok(None) => break,     // end of stream
            Err(_status) => break, // gRPC error closes the stream
        };
        for entry in message.entries.into_iter() {
            let (path, datapoint): (String, Datapoint) = entry;
            let Some(signal) = datapoint_to_signal(&path, datapoint) else {
                continue;
            };
            // If `send` errors, the receiver has been dropped — fold
            // up the task. Back-pressure (channel full) parks us
            // here, which is exactly the desired behavior.
            if tx.send(signal).await.is_err() {
                return;
            }
        }
    }
}

/// Converts a KUKSA Datapoint into a [`VssSignal`]. Returns `None`
/// for datapoints with no value, unsupported types, or non-coercible
/// numeric ranges.
fn datapoint_to_signal(path: &str, datapoint: Datapoint) -> Option<VssSignal> {
    let timestamp_ms = datapoint
        .timestamp
        .as_ref()
        .map(timestamp_to_ms)
        .unwrap_or_else(now_ms);
    let value = datapoint.value.and_then(value_to_vss)?;
    Some(VssSignal::new(path.to_string(), value, timestamp_ms))
}

/// Converts a `prost_types::Timestamp` to milliseconds since the
/// UNIX epoch. Returns `0` if the conversion would overflow `i64`.
fn timestamp_to_ms(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds
        .checked_mul(1000)
        .and_then(|ms| ms.checked_add((ts.nanos / 1_000_000) as i64))
        .unwrap_or(0)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Lifts a KUKSA `Value` (oneof) onto our scalar `VssValue`. Array
/// variants and `Uint64` values too large for `i64` are dropped.
fn value_to_vss(value: Value) -> Option<VssValue> {
    let typed = value.typed_value?;
    match typed {
        TypedValue::String(s) => Some(VssValue::String(s)),
        TypedValue::Bool(b) => Some(VssValue::Bool(b)),
        TypedValue::Int32(v) => Some(VssValue::Int(v as i64)),
        TypedValue::Int64(v) => Some(VssValue::Int(v)),
        TypedValue::Uint32(v) => Some(VssValue::Int(v as i64)),
        TypedValue::Uint64(v) => i64::try_from(v).ok().map(VssValue::Int),
        TypedValue::Float(v) => Some(VssValue::Float(v as f64)),
        TypedValue::Double(v) => Some(VssValue::Float(v)),
        TypedValue::StringArray(_)
        | TypedValue::BoolArray(_)
        | TypedValue::Int32Array(_)
        | TypedValue::Int64Array(_)
        | TypedValue::Uint32Array(_)
        | TypedValue::Uint64Array(_)
        | TypedValue::FloatArray(_)
        | TypedValue::DoubleArray(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-only smoke test: the source is `Send + Sync` and
    /// dyn-objectifiable through the trait.
    #[test]
    fn type_plumbing_compiles_and_objectifies() {
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<KuksaVssSignalSource>();
        fn _to_dyn(s: KuksaVssSignalSource) -> std::sync::Arc<dyn VssSignalSource> {
            std::sync::Arc::new(s)
        }
        let _ = _to_dyn;
    }

    #[test]
    fn timestamp_conversion_handles_seconds_and_nanos() {
        let ts = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 500_000_000,
        };
        assert_eq!(timestamp_to_ms(&ts), 1_700_000_000_500);
    }

    #[test]
    fn timestamp_overflow_yields_zero() {
        let ts = prost_types::Timestamp {
            seconds: i64::MAX / 1000 + 1,
            nanos: 0,
        };
        assert_eq!(timestamp_to_ms(&ts), 0);
    }

    #[test]
    fn value_lifts_string_and_bool() {
        let s = value_to_vss(Value {
            typed_value: Some(TypedValue::String("hi".into())),
        });
        assert_eq!(s, Some(VssValue::String("hi".into())));
        let b = value_to_vss(Value {
            typed_value: Some(TypedValue::Bool(true)),
        });
        assert_eq!(b, Some(VssValue::Bool(true)));
    }

    #[test]
    fn value_lifts_integer_variants_to_i64() {
        for typed in [
            TypedValue::Int32(-7),
            TypedValue::Int64(-7),
            TypedValue::Uint32(0xFFFF_FFFFu32),
        ] {
            let value = value_to_vss(Value {
                typed_value: Some(typed),
            });
            assert!(matches!(value, Some(VssValue::Int(_))));
        }
    }

    #[test]
    fn value_drops_oversized_u64() {
        let too_big = TypedValue::Uint64((i64::MAX as u64) + 1);
        let value = value_to_vss(Value {
            typed_value: Some(too_big),
        });
        assert_eq!(value, None);
    }

    #[test]
    fn value_lifts_floats() {
        let f = value_to_vss(Value {
            typed_value: Some(TypedValue::Float(0.5)),
        });
        assert!(matches!(f, Some(VssValue::Float(_))));
        let d = value_to_vss(Value {
            typed_value: Some(TypedValue::Double(2.5)),
        });
        assert!(matches!(d, Some(VssValue::Float(_))));
    }

    #[test]
    fn value_drops_array_variants() {
        let arrays = [
            TypedValue::StringArray(kuksa_rust_sdk::v2_proto::StringArray { values: vec![] }),
            TypedValue::BoolArray(kuksa_rust_sdk::v2_proto::BoolArray { values: vec![] }),
        ];
        for typed in arrays {
            let value = value_to_vss(Value {
                typed_value: Some(typed),
            });
            assert_eq!(value, None);
        }
    }

    #[test]
    fn datapoint_with_no_value_yields_none() {
        let dp = Datapoint {
            timestamp: None,
            value: None,
        };
        assert!(datapoint_to_signal("Vehicle.Speed", dp).is_none());
    }

    #[test]
    fn datapoint_with_value_yields_signal_at_timestamp() {
        let dp = Datapoint {
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            value: Some(Value {
                typed_value: Some(TypedValue::Float(72.0)),
            }),
        };
        let signal = datapoint_to_signal("Vehicle.Speed", dp).expect("signal");
        assert_eq!(signal.path, "Vehicle.Speed");
        assert_eq!(signal.timestamp, 1_700_000_000_000);
        assert_eq!(signal.value, VssValue::Float(72.0));
    }

    /// Verifies `connect` surfaces a clean error rather than panicking
    /// when no Databroker is reachable.
    #[tokio::test]
    async fn connect_to_dead_endpoint_yields_clean_error() {
        // Port 1 is reserved and almost always closed.
        let config = KuksaConnectConfig::new("http://127.0.0.1:1", ["Vehicle.Speed"]);
        let result = KuksaVssSignalSource::connect(config).await;
        assert!(result.is_err());
    }

    /// Live round-trip against a real KUKSA.val Databroker. Skipped
    /// unless `KUKSA_HOST` is set; `#[ignore]`'d so it never fires
    /// in regular test runs.
    #[tokio::test]
    #[ignore = "requires a running KUKSA.val Databroker reachable at $KUKSA_HOST"]
    async fn kuksa_live_subscription_delivers_signals() {
        let Ok(host) = std::env::var("KUKSA_HOST") else {
            eprintln!("KUKSA_HOST not set — skipping live test");
            return;
        };
        let config = KuksaConnectConfig::new(host, ["Vehicle.Speed"]);
        let source = KuksaVssSignalSource::connect(config)
            .await
            .expect("kuksa connect");
        // KUKSA always sends a current-value frame at subscribe time,
        // so a fresh source should yield at least one signal within
        // a few seconds even if the value is None on the upstream side.
        let recv = tokio::time::timeout(std::time::Duration::from_secs(5), source.next()).await;
        match recv {
            Ok(Some(s)) => assert_eq!(s.path, "Vehicle.Speed"),
            Ok(None) => panic!("stream closed before first message"),
            Err(_) => panic!("no signal arrived within 5s; is the Databroker producing values?"),
        }
    }
}
