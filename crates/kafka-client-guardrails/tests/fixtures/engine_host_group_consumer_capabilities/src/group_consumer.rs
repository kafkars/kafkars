//! Deliberately steals policy, runtime, transport, and public scheduling capabilities.

use crate::{Engine, admin, exports, producer, protocol, transaction};
use kafka_driver;
use kafka_wire;
use kafka_wire_core;
use kafka_wire_records;
use std::{
    future::Future,
    net::TcpStream,
    sync::{Condvar, Mutex, RwLock},
    thread,
    time::{Instant, SystemTime},
};

async fn steal<T>(owner: &T) {
    let _socket = TcpStream::connect("127.0.0.1:1");
    let _worker = thread::spawn(|| ());
    let _now = Instant::now();
    let _wall = SystemTime::now();
    let _condvar = Condvar::new();
    let _mutex = Mutex::new(());
    let _rwlock = RwLock::new(());
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

fn retain_future<T: Future>(_future: T) {}
