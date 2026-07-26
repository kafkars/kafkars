//! Deliberate theft of every forbidden Fetch activation capability.

use crate::clock::MonotonicClock;
use crate::driver;
use crate::protocol;
use kafka_driver as raw_driver;
use std::future::Future;
use std::net::TcpStream;
use std::thread;
use std::time;

fn steal<T>(
    _clock: MonotonicClock,
    _driver: driver::DriverOwner,
    _protocol: protocol::consumer::PreparedFetchRequest,
    _raw_driver: raw_driver::Driver,
    _wire: kafka_wire::FetchResponse,
    _wire_core: kafka_wire_core::DecodeError,
    _attempt: FetchAttemptDeadline,
    _operation: OperationDeadline,
    _executor: DirectFetchExecutor,
    _timers: AssignedTimers,
    _assigned_delivery: AssignedConsumerDelivery,
    _delivery: FetchDelivery,
    _delivery_store: FetchDeliveryStore,
    _future: &dyn Future<Output = ()>,
    _network: TcpStream,
    _thread: thread::Thread,
    _duration: time::Duration,
    _tokio: tokio::runtime::Runtime,
) {}

async fn hidden_executor() {}
