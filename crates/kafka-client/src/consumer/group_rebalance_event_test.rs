//! Public classic-group transition and revocation-completion contract.

use super::{ConsumerAssignment, ConsumerAssignmentPartition, ConsumerEvent, ConsumerRevocation};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn assignment_event_retains_named_epoch_fence_without_clone_authority() {
    let assignment = ConsumerAssignment::from_parts(
        7,
        vec![ConsumerAssignmentPartition::from_parts(
            "orders".to_owned(),
            3,
        )],
    );
    let event = ConsumerEvent::PartitionsAssigned(assignment);
    let ConsumerEvent::PartitionsAssigned(assignment) = event else {
        panic!("assigned event expected");
    };

    assert_eq!(assignment.assignment_epoch(), 7);
    assert_eq!(assignment.partitions()[0].topic(), "orders");
    assert_eq!(assignment.partitions()[0].partition(), 3);
    assert_not_impl!(ConsumerEvent: Clone);
    assert_not_impl!(ConsumerEvent: Copy);
}

#[test]
fn revoking_event_is_linear_and_owns_completion() {
    fn require_assignment(_assignment: fn(&ConsumerRevocation) -> &ConsumerAssignment) {}
    fn require_partitions(
        _partitions: fn(&ConsumerRevocation) -> &[super::ConsumerAssignmentPartition],
    ) {
    }
    fn require_complete(_complete: fn(&mut ConsumerRevocation) -> Result<(), crate::KafkaError>) {}

    require_assignment(ConsumerRevocation::assignment);
    require_partitions(ConsumerRevocation::partitions);
    require_complete(ConsumerRevocation::complete);
    assert_not_impl!(ConsumerRevocation: Clone);
    assert_not_impl!(ConsumerRevocation: Copy);
}
