//! Public capture ordering, exact rejection, and accepted-fault contracts.

use std::time::Duration;

use super::{
    GroupConsumerCommitAdmissionErrorKind, GroupConsumerCommitDeliveryStatus,
    GroupConsumerCommitFailureKind, GroupConsumerCommitOutcome, test_support::GroupCommitFixture,
};
use crate::consumer::group_batch::GroupConsumerCheckpointObservation;

#[test]
fn capture_failure_precedes_closed_admission_and_returns_the_exact_checkpoint() {
    let mut fixture = GroupCommitFixture::start(false);
    let checkpoint = fixture.take_checkpoint();
    let identity = checkpoint.storage_identity();
    fixture.owner.close_admission();

    let error = fixture
        .handle
        .try_commit(checkpoint, Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must reject"));

    assert_eq!(
        error.kind(),
        GroupConsumerCommitAdmissionErrorKind::InvalidDeadline
    );
    assert_eq!(error.into_checkpoint().storage_identity(), identity);
    fixture.finish();
}

#[test]
fn contended_admission_returns_the_exact_checkpoint_without_transfer() {
    let mut fixture = GroupCommitFixture::start(false);
    let checkpoint = fixture.take_checkpoint();
    let identity = checkpoint.storage_identity();
    let registry = fixture.owner.lock_registry_for_test();

    let error = fixture
        .handle
        .try_commit(checkpoint, Duration::from_secs(1))
        .err()
        .unwrap_or_else(|| panic!("contended admission must reject"));

    assert_eq!(
        error.kind(),
        GroupConsumerCommitAdmissionErrorKind::Contended
    );
    assert_eq!(error.into_checkpoint().storage_identity(), identity);
    drop(registry);
    fixture.finish();
}

#[test]
fn accepted_wake_fault_retains_the_sole_terminal_observer() {
    let mut fixture = GroupCommitFixture::start(true);
    let checkpoint = fixture.take_checkpoint();
    let accepted = fixture
        .handle
        .try_commit(checkpoint, Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("commit admission: {error}"));

    assert!(!accepted.host_faulted());
    assert!(accepted.wake_failed());
    let observer = accepted.into_observer();
    let mut registry = fixture.owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let outcome = observer
        .wait()
        .unwrap_or_else(|error| panic!("commit observation: {error}"));
    let GroupConsumerCommitOutcome::Failed(failure, _checkpoint) = outcome else {
        panic!("queued shutdown recovery must fail definitely unsent");
    };
    assert_eq!(
        failure.kind(),
        GroupConsumerCommitFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        GroupConsumerCommitDeliveryStatus::NotSent
    );
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn checkpoint_observation_shape_remains_private_and_exact() {
    fn shape(observation: GroupConsumerCheckpointObservation) {
        let _: &str = observation.topic();
        let _: i32 = observation.partition();
        drop(observation);
    }
    let _ = shape as fn(GroupConsumerCheckpointObservation);
}
