//! Forbidden policy, runtime, and coordinator-routing capabilities.

use std::{future::Future, net::TcpStream, thread, time::Instant};

use crate::protocol;

async fn steal<T>(driver: &T) {
    let _socket = TcpStream::connect("127.0.0.1:1");
    let _now = Instant::now();
    let _worker = thread::spawn(|| ());
    let _retry = Retry;
    let _input = ClassicGroupInput;
    let _effect = ClassicGroupEffect;
    let _machine = ClassicGroupMachine;
    let _route = Route::Coordinator;
    let _normalized = normalize(protocol);
    driver.invalidate();
}

struct Retry;

fn retain_future<T: Future>(_future: T) {}
