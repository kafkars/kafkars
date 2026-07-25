//! Deliberately steals policy, runtime, transport, and public shutdown capabilities.

use crate::{Engine, admin, driver, exports, producer, protocol, transaction};
use kafka_client_core;
use kafka_driver;
use kafka_wire;
use kafka_wire_core;
use kafka_wire_records;
use std::{thread, time};

async fn steal<T>(owner: &T) {
    let _worker = thread::spawn(|| ());
    let _duration = time::Duration::ZERO;
    let _callback = Callback;
    let _metadata = Metadata;
    let _deadline = OperationDeadline;
    let _retry = Retry;
    let _route = Route::AnyBroker;
    let _runtime = Runtime;
    let _started = StartedEngineHost;
    let _traffic = TrafficClass;
    let _engine = Engine;
    let _tokio = tokio::spawn(async {});
    let _async_std = async_std::task::spawn(async {});
    let _smol = smol::spawn(async {});
    owner.invalidate();
}

struct Callback;
struct Metadata;
struct OperationDeadline;
struct Retry;
struct Runtime;
struct StartedEngineHost;
struct TrafficClass;
enum Route {
    AnyBroker,
}
