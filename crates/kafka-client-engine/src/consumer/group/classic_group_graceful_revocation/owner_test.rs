//! Exact event, completion, deadline, and owner-loss behavior.

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition,
    AssignmentEpoch, AssignmentGeneration, ClassicGeneration, ClassicGracefulRevocationLease,
    ClassicGracefulRevocationLossReason, ClassicGracefulRevocationTerminal, Deadline,
    GroupAssignmentPartition, GroupId, LiveGroupAssignment, MemberId, Moment, PartitionIndex,
    StartPosition, TopicId,
};

use super::{ClassicGroupRevocationAcknowledgeError, ClassicGroupRevocationOwner};

#[test]
fn exact_epoch_acknowledges_before_absolute_deadline() {
    let epoch = assignment_epoch();
    let mut owner = begun_owner(epoch, Deadline::from_tick(20));

    assert_eq!(owner.acknowledge(epoch, Moment::from_tick(19)), Ok(()));
    assert!(matches!(
        owner.terminal(),
        Some(ClassicGracefulRevocationTerminal::Acknowledged(lease))
            if lease.assignment_epoch() == epoch
                && lease.deadline() == Deadline::from_tick(20)
    ));
}

#[test]
fn public_acknowledgement_maps_membership_to_the_private_fetch_lease() {
    let epoch = assignment_epoch();
    let mut owner = begun_owner(epoch, Deadline::from_tick(20));
    assert_eq!(epoch.get(), 1);
    assert_eq!(
        owner.acknowledge_public(epoch.get(), Moment::from_tick(19)),
        Err(ClassicGroupRevocationAcknowledgeError::AssignmentEpochMismatch)
    );
    assert!(owner.terminal().is_none());
    assert_eq!(owner.acknowledge_public(7, Moment::from_tick(19)), Ok(()));
    assert!(matches!(
        owner.terminal(),
        Some(ClassicGracefulRevocationTerminal::Acknowledged(lease))
            if lease.assignment_epoch() == epoch
    ));
}

#[test]
fn deadline_and_owner_loss_preclude_late_success() {
    let epoch = assignment_epoch();
    let mut expired = begun_owner(epoch, Deadline::from_tick(20));
    assert_eq!(expired.expire_if_due(Moment::from_tick(20)), Ok(true));
    assert!(matches!(
        expired.terminal(),
        Some(ClassicGracefulRevocationTerminal::Lost {
            reason: ClassicGracefulRevocationLossReason::DeadlineElapsed,
            ..
        })
    ));
    assert!(matches!(
        expired.acknowledge(epoch, Moment::from_tick(20)),
        Err(ClassicGroupRevocationAcknowledgeError::Core(_))
    ));

    let mut lost = begun_owner(epoch, Deadline::from_tick(20));
    assert_eq!(lost.lose_owner(), Ok(true));
    assert!(matches!(
        lost.terminal(),
        Some(ClassicGracefulRevocationTerminal::Lost {
            reason: ClassicGracefulRevocationLossReason::OwnerLost,
            ..
        })
    ));
    assert!(matches!(
        lost.acknowledge(epoch, Moment::from_tick(10)),
        Err(ClassicGroupRevocationAcknowledgeError::Core(_))
    ));
}

#[test]
fn acknowledgment_at_deadline_reports_loss_instead_of_success() {
    let epoch = assignment_epoch();
    let mut owner = begun_owner(epoch, Deadline::from_tick(20));

    assert_eq!(
        owner.acknowledge(epoch, Moment::from_tick(20)),
        Err(ClassicGroupRevocationAcknowledgeError::DeadlineElapsed)
    );
    assert!(matches!(
        owner.terminal(),
        Some(ClassicGracefulRevocationTerminal::Lost {
            reason: ClassicGracefulRevocationLossReason::DeadlineElapsed,
            ..
        })
    ));
}

fn begun_owner(epoch: AssignmentEpoch, deadline: Deadline) -> ClassicGroupRevocationOwner {
    let mut owner = ClassicGroupRevocationOwner::new();
    owner
        .begin(
            assignment(),
            ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("classic generation")),
            ClassicGracefulRevocationLease::new(epoch, deadline),
            Moment::from_tick(3),
        )
        .unwrap_or_else(|(error, _assignment)| panic!("begin failed: {error:?}"));
    owner
}

fn assignment_epoch() -> AssignmentEpoch {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("assignment failed: {error}"))
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assignment epoch expected"))
}

fn assignment() -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group")),
        MemberId::try_from_raw(5).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(7).unwrap_or_else(|| panic!("assignment generation")),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        )],
    )
    .unwrap_or_else(|error| panic!("live assignment: {error:?}"))
}
