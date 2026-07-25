//! Capabilities forbidden from deterministic group-commit policy.

use bytes::Bytes;
use kafka_client_engine::Engine;
use kafka_driver::Driver;
use std::future::Future;

fn intrude(
    _assigned: AssignedConsumer,
    _machine: AssignedConsumerMachine,
    _callback: Callback,
    _clock: Clock,
    _coordinator: Coordinator,
    _engine: Engine,
    _future: &dyn Future<Output = ()>,
    _generated: Generated,
    _group_coordinator: GroupCoordinator,
    _metadata: Metadata,
    _retry: Retry,
    _text: String,
    _transport: Transport,
    _wire: Wire,
    _bytes: Bytes,
    _raw_driver: Driver,
    _wire_message: kafka_wire::OffsetCommitRequest,
    _async_std: async_std::Task,
    _smol: smol::Task<()>,
    _tokio: tokio::task::JoinHandle<()>,
) {
    let _now = std::time::Instant::now();
}

async fn hidden_executor() {}
