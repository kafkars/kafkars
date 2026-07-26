use crate::Deadline;
use crate::consumer::group_position;
use kafka_client_engine::Engine;
use kafka_driver::Driver;
use kafka_wire::FetchRequest;
use std::future::Future;
use std::time::Instant;

fn forbidden(
    _deadline: Deadline,
    _bootstrap: GroupPositionBootstrapMachine,
    _resolution: PositionResolution,
    _start: StartPosition,
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
