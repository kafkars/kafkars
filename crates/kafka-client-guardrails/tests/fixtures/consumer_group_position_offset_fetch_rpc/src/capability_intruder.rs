//! Forbidden admin, generic executor, runtime, and route capabilities.

use std::{net::TcpStream, thread, time::Instant};

async fn steal<T>(driver: &T) {
    let _admin = crate::admin::GroupOffsetsCall;
    let _operation = crate::operation::OperationId;
    let _callback = Callback;
    let _executor = Executor;
    let _clock = Instant::now();
    let _socket = TcpStream::connect("127.0.0.1:1");
    let _worker = thread::spawn(|| ());
    let _retry = Retry;
    let _lane = TrafficClass::LongPoll;
    let _runtime = tokio::spawn(async {});
    driver.invalidate();
    unsafe {}
}

struct Callback;
struct Executor;
struct Retry;
struct TrafficClass;

impl TrafficClass {
    const LongPoll: Self = Self;
}
