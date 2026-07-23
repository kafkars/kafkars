//! Accepted `CreateTopics` ownership and post-commit fault scenarios.

use super::{CreateTopicsAcceptedFaultKind, CreateTopicsHostError, handle::accepted_fault_kind};

#[test]
fn accepted_wake_failure_keeps_ownership_and_an_exact_fault_category() {
    assert_eq!(
        accepted_fault_kind(CreateTopicsHostError::Wake),
        CreateTopicsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(CreateTopicsHostError::MissingTerminal),
        CreateTopicsAcceptedFaultKind::HostInvariant
    );
}
