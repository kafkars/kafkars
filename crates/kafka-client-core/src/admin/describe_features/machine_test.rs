//! Lifecycle construction and state vocabulary scenarios.

use crate::{Deadline, OperationId};

use super::{DescribeFeaturesMachine, DescribeFeaturesState};

#[test]
fn accepted_machine_starts_ready_after_capacity_reservation() {
    let machine =
        DescribeFeaturesMachine::new(OperationId::from_raw(57), Deadline::from_tick(1_000));
    assert_eq!(machine.state(), DescribeFeaturesState::Ready);
}
