//! Accepted ownership and fault scenarios for `DescribeConfigs`.

use super::{
    DescribeConfigsAcceptedFaultKind, DescribeConfigsHostError, handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_operation_ownership() {
    assert_eq!(
        accepted_fault_kind(DescribeConfigsHostError::Wake),
        DescribeConfigsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(DescribeConfigsHostError::MissingTerminal),
        DescribeConfigsAcceptedFaultKind::HostInvariant
    );
}
