//! Runtime, transport, and fresh-timeout theft forbidden by this fixture.

use kafka_driver as raw_driver;
use std::{
    future::Future,
    sync::{Condvar, Mutex, RwLock},
    thread,
    time::{Duration, Instant},
};

fn intrude<T>(
    owner: T,
    _admin: crate::admin::Owner,
    _clock: crate::clock::Owner,
    _completion: crate::completion::Owner,
    _host: crate::host::Owner,
    _producer: crate::producer::Owner,
    _protocol: crate::protocol::Owner,
    _transaction: crate::transaction::Owner,
    _driver: raw_driver::Driver,
    _wire: kafka_wire::Metadata,
    _wire_core: kafka_wire_core::DecodeError,
    _records: kafka_wire_records::RecordBatch,
    _tokio: tokio::runtime::Runtime,
    _async_std: async_std::task::JoinHandle<()>,
    _smol: smol::Task<()>,
    _future: &dyn Future<Output = ()>,
    _network: std::net::TcpStream,
    _thread: std::thread::Thread,
    _condvar: Condvar,
    _mutex: Mutex<()>,
    _rwlock: RwLock<()>,
    _callback: Callback,
    _metadata: Metadata,
    _transport: Transport,
    _retry: Retry,
    _route: Route,
    _capture: DeadlineCapture,
) {
    let _worker = thread::spawn(|| ());
    let _now = Instant::now();
    owner.capture_deadline_after(Duration::from_nanos(1));
    OperationDeadline::from_boundary_parts();
    invalidate();
}

async fn hidden_executor() {}

struct Callback;
struct DeadlineCapture;
struct Metadata;
struct Retry;
struct Route;
struct Transport;
