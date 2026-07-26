//! Capabilities forbidden from deterministic group position bootstrap.

use bytes::Bytes;
use kafka_client_engine::Engine;
use kafka_driver::Driver;
use std::future::Future;
use std::io::Read;
use std::net::TcpStream;

fn intrude(
    _operation: crate::operation::GenericOperation,
    _assigned: AssignedConsumer,
    _machine: AssignedConsumerMachine,
    _callback: Callback,
    _clock: Clock,
    _coordinator: Coordinator,
    _engine: Engine,
    _fetch_fence: FetchFence,
    _fetch_ready: FetchReady,
    _future: &dyn Future<Output = ()>,
    _generated: Generated,
    _metadata: Metadata,
    _position_fence: PositionFence,
    _position_resolution: PositionResolution,
    _retry: Retry,
    _runtime: Runtime,
    _text: String,
    _wire: Wire,
    _bytes: Bytes,
    _raw_driver: Driver,
    _wire_message: kafka_wire::OffsetFetchRequest,
    _async_std: async_std::Task,
    _smol: smol::Task<()>,
    _tokio: tokio::task::JoinHandle<()>,
    _read: &dyn Read,
    _socket: TcpStream,
) {
    let _now = std::time::Instant::now();
}

async fn hidden_executor() {}
