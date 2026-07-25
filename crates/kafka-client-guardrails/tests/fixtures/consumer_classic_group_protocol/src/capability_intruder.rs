//! Forbidden policy, runtime, transport, and sibling-domain capabilities.

use std::{
    future::Future,
    net::TcpStream,
    sync::{Condvar, Mutex, RwLock},
    thread,
    time::Instant,
};

use crate::{admin, clock, consumer, driver, producer, transaction};
use kafka_driver;
use kafka_wire_records;

async fn steal() {
    let _socket = TcpStream::connect("127.0.0.1:1");
    let _now = Instant::now();
    let _worker = thread::spawn(|| ());
    let _mutex = Mutex::new(());
    let _rwlock = RwLock::new(());
    let _condvar = Condvar::new();
    let _retry = Retry;
    let _transport = Transport;
    retain_future(async {});
    let _ = (
        admin,
        clock,
        consumer,
        driver,
        producer,
        transaction,
        kafka_driver,
        kafka_wire_records,
    );
}

struct Retry;
struct Transport;

fn retain_future<T: Future>(_future: T) {}
