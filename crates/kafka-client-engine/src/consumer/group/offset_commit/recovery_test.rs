//! Shutdown recovery refusal for retained host faults.

use super::host::{
    GroupOffsetCommitHost, GroupOffsetCommitHostError, GroupOffsetCommitPreparationFault,
};

#[test]
fn unmeasurable_preparation_fault_still_blocks_recovery_and_shutdown() {
    let mut host =
        GroupOffsetCommitHost::start().unwrap_or_else(|error| panic!("host start: {error}"));
    host.preparation_fault = Some(GroupOffsetCommitPreparationFault::RetainedByteOverflowForTest);

    assert_eq!(
        host.recover_after_driver_shutdown(),
        Err(GroupOffsetCommitHostError::Preparation)
    );
    assert!(matches!(
        host.finish_shutdown(),
        Err(GroupOffsetCommitHostError::Unsettled)
    ));

    host.preparation_fault = None;
    host.fault = None;
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
