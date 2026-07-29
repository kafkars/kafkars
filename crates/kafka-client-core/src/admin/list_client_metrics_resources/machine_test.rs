//! Lifecycle construction and state vocabulary scenarios.

use crate::{Deadline, OperationId};

use super::{ListClientMetricsResourcesMachine, ListClientMetricsResourcesState};

#[test]
fn accepted_machine_starts_ready_after_capacity_reservation() {
    let machine =
        ListClientMetricsResourcesMachine::new(OperationId::from_raw(41), Deadline::from_tick(900));
    assert_eq!(machine.state(), ListClientMetricsResourcesState::Ready);
}
