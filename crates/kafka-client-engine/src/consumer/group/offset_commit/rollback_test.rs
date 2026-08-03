//! Completion and byte rollback when deterministic admission rejects.

use kafka_client_core::{
    AssignmentGeneration, GroupCheckpoint, GroupOffsetCommitAdmissionErrorKind,
};

use crate::consumer::GroupConsumerProtocol;

use super::{
    host::GroupOffsetCommitHost,
    test_support::{catalog, checkpoint, deadline},
};

#[test]
fn stale_checkpoint_rejection_rolls_back_completion_and_bytes() {
    let catalog = catalog();
    let current = checkpoint(&catalog);
    let stale_generation = AssignmentGeneration::try_from_raw(2)
        .unwrap_or_else(|| panic!("stale generation must be nonzero"));
    let stale = GroupCheckpoint::try_new(
        current.group_id(),
        current.member_id(),
        stale_generation,
        current.entries().to_vec(),
    )
    .unwrap_or_else(|error| panic!("stale checkpoint: {error}"));
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    let failure = host
        .try_admit(
            GroupConsumerProtocol::Classic,
            &catalog,
            deadline(40),
            stale,
        )
        .err()
        .unwrap_or_else(|| panic!("stale checkpoint must fail"));

    assert_eq!(
        failure.kind,
        super::admission::GroupOffsetCommitAdmissionFailureKind::Core(
            GroupOffsetCommitAdmissionErrorKind::GenerationMismatch
        )
    );
    assert_eq!(failure.checkpoint.assignment_generation(), stale_generation);
    assert_eq!(host.retained_bytes_for_test(), 0);
    assert!(host.operations.is_empty());

    host.close_admission();
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
