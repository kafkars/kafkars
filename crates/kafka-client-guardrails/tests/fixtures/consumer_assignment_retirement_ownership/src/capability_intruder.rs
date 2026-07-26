use crate::consumer::group_position;
use crate::{Deadline, Moment};
use kafka_client_engine::Engine;
use kafka_driver::Driver;
use kafka_wire::FetchRequest;
use std::future::Future;
use std::time::Instant;

fn forbidden(
    _deadline: Deadline,
    _moment: Moment,
    _operation_deadline: OperationDeadline,
    _bootstrap: GroupPositionBootstrapMachine,
    _byte: Byte,
    _bytes: Bytes,
    _raw: u8,
    _callback: Callback,
    _clock: Clock,
    _future: Future,
    _runtime: Runtime,
    _engine: Engine,
    _driver: Driver,
    _request: FetchRequest,
    _instant: Instant,
) {
    let _group = group_position::GroupPositionBootstrapMachine;
    let _future = async {};
}
