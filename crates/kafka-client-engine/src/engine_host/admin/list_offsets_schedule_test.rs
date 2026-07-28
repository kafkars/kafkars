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
