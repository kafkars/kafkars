//! Accepted `DescribeCluster` ownership and fault scenarios.

use super::{
    DescribeClusterAcceptedFaultKind, DescribeClusterHostError,
    describe_handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_operation_ownership() {
    assert_eq!(
        accepted_fault_kind(DescribeClusterHostError::Wake),
        DescribeClusterAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(DescribeClusterHostError::MissingTerminal),
        DescribeClusterAcceptedFaultKind::HostInvariant
    );
}
