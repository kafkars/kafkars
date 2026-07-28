//! Shared-schedule accounting for one Admin `ListOffsets` turn.

use kafka_client_core::Deadline;

use super::{
    list_offsets::AdminListOffsetsProgress, list_offsets_schedule::extend, schedule::AdminProgress,
};

#[test]
fn extension_preserves_bounded_progress_and_the_earliest_deadline() {
    let mut progress = AdminProgress {
        unsettled: 7,
        driver_progress: false,
        next_deadline: Some(Deadline::from_tick(31)),
    };
    let list_offsets = AdminListOffsetsProgress {
        unsettled: 2,
        driver_progress: true,
        next_deadline: Some(Deadline::from_tick(19)),
    };

    extend(&mut progress, &list_offsets);

    assert_eq!(progress.unsettled, 9);
    assert!(progress.driver_progress);
    assert_eq!(progress.next_deadline, Some(Deadline::from_tick(19)));
}

#[test]
fn partition_listing_extension_preserves_bounded_progress_and_deadline() {
    let mut progress = AdminProgress {
        unsettled: 5,
        driver_progress: false,
        next_deadline: Some(Deadline::from_tick(29)),
    };
    let listing = super::list_partition_reassignments::ListPartitionReassignmentsProgress {
        unsettled: 3,
        driver_progress: true,
        next_deadline: Some(Deadline::from_tick(17)),
    };

    super::list_offsets_schedule::extend_partition_reassignments(&mut progress, &listing);

    assert_eq!(progress.unsettled, 8);
    assert!(progress.driver_progress);
    assert_eq!(progress.next_deadline, Some(Deadline::from_tick(17)));
}

#[test]
fn partition_alteration_extension_preserves_bounded_progress_and_deadline() {
    let mut progress = AdminProgress {
        unsettled: 4,
        driver_progress: false,
        next_deadline: Some(Deadline::from_tick(23)),
    };
    let alteration = super::alter_partition_reassignments::AlterPartitionReassignmentsProgress {
        unsettled: 2,
        driver_progress: true,
        next_deadline: Some(Deadline::from_tick(13)),
    };

    super::list_offsets_schedule::extend_partition_reassignment_alterations(
        &mut progress,
        &alteration,
    );

    assert_eq!(progress.unsettled, 6);
    assert!(progress.driver_progress);
    assert_eq!(progress.next_deadline, Some(Deadline::from_tick(13)));
}
