//! Host lifecycle and unhealthy-shutdown scenarios.

use super::host::{GroupOffsetCommitHost, GroupOffsetCommitHostError};

#[test]
fn latched_fault_prevents_successful_notifier_shutdown() {
    let mut host =
        GroupOffsetCommitHost::start().unwrap_or_else(|error| panic!("host start: {error}"));
    host.close_admission();
    host.fault = Some(GroupOffsetCommitHostError::ByteAccounting);

    assert!(matches!(
        host.finish_shutdown(),
        Err(GroupOffsetCommitHostError::Unsettled)
    ));

    host.fault = None;
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
