//! Foreign domain, allocation, transport, and runtime theft fixture.

use std::{future::Future, time::Instant};

fn intrude(
    _admin: crate::admin::Policy,
    _consumer: crate::consumer::Policy,
    _producer: crate::producer::Policy,
    _engine: kafka_client_engine::Engine,
    _engine_marker: Engine,
    _driver: kafka_driver::Driver,
    _wire: kafka_wire::Request,
    _bytes: bytes::Bytes,
    _future: &dyn Future<Output = ()>,
    _string: String,
    _callback: Callback,
    _clock: Clock,
    _coordinator: Coordinator,
    _generated: Generated,
    _metadata: Metadata,
    _retry: Retry,
    _runtime: Runtime,
    _transport: Transport,
    _wire_owner: Wire,
) {
    let _now = Instant::now();
    let _tokio = tokio::spawn;
    let _async_std = async_std::task::spawn;
    let _smol = smol::spawn;
}

async fn hidden_executor() {}

struct Callback;
struct Clock;
struct Coordinator;
struct Engine;
struct Generated;
struct Metadata;
struct Retry;
struct Runtime;
struct Transport;
struct Wire;
