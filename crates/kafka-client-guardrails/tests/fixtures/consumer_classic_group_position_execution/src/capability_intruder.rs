//! Raw wire, driver, runtime, and unsafe capability theft.

use kafka_driver as raw_driver;
use std::{future::Future, net::TcpStream, thread};

fn steal(
    _driver: raw_driver::Driver,
    _wire: kafka_wire::OffsetFetchResponse,
    _wire_core: kafka_wire_core::DecodeError,
    _future: &dyn Future<Output = ()>,
    _network: TcpStream,
    _thread: thread::Thread,
    _tokio: tokio::runtime::Runtime,
) {
}

async fn hidden_executor() {}
