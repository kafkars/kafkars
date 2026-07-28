//! Accepted client-quota description lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{DescribeClientQuotasMachine, DescribeClientQuotasPlan, DescribeClientQuotasState};

#[test]
fn accepted_machine_begins_ready() {
    let machine = DescribeClientQuotasMachine::new(
        OperationId::from_raw(48),
        Deadline::from_tick(100),
        DescribeClientQuotasPlan::new(Vec::new(), false)
            .unwrap_or_else(|error| panic!("valid filter: {error}")),
    );

    assert_eq!(machine.state(), DescribeClientQuotasState::Ready);
}
