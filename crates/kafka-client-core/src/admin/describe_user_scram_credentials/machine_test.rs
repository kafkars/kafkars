//! Accepted SCRAM credential description lifecycle scenarios.

use crate::{Deadline, OperationId};

use super::{
    DescribeUserScramCredentialsMachine, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsState,
};

#[test]
fn accepted_machine_begins_ready() {
    let machine = DescribeUserScramCredentialsMachine::new(
        OperationId::from_raw(50),
        Deadline::from_tick(100),
        DescribeUserScramCredentialsPlan::new(None)
            .unwrap_or_else(|error| panic!("valid selection: {error}")),
    );

    assert_eq!(machine.state(), DescribeUserScramCredentialsState::Ready);
}
