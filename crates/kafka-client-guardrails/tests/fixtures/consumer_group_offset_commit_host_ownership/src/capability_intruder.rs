//! Deliberately reaches raw transport, runtime, retry, and invalidation seams.

use kafka_driver;
use kafka_wire;
use kafka_wire_core;
use kafka_wire_records;
use std::{future::Future, net::TcpStream, thread};

async fn steal<T>(driver: &T) {
    let _socket = TcpStream::connect("127.0.0.1:1");
    let _worker = thread::spawn(|| ());
    let _callback = Callback;
    let _retry = Retry;
    driver.invalidate();
}

struct Callback;
struct Retry;

fn retain_future<T: Future>(_future: T) {}
