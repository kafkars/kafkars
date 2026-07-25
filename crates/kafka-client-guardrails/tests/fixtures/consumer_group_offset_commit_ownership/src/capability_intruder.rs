//! Forbidden runtime, clock, retry, and invalidation capabilities.

use std::{future::Future, net::TcpStream, thread, time::Instant};

async fn steal<T>(driver: &T) {
    let _socket = TcpStream::connect("127.0.0.1:1");
    let _now = Instant::now();
    let _worker = thread::spawn(|| ());
    let _retry = Retry;
    driver.invalidate();
}

struct Retry;

fn retain_future<T: Future>(_future: T) {}
