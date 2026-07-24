//! Cross-admin fairness and shutdown-quiescence scenarios.

use std::cell::Cell;

use kafka_client_core::{Deadline, Moment};

use super::{
    create_topics::CreateTopicsProgress,
    delete_topics::DeleteTopicsProgress,
    schedule::{combine, drive_create_then_capture_delete},
};
use crate::protocol::admin::delete_topics::remaining_timeout_ms;

#[test]
fn saturated_create_lane_cannot_hide_runnable_delete_work() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(7)),
        },
    );
    assert_eq!(combined.unsettled, usize::MAX);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(7)));
}

#[test]
fn either_concrete_owner_prevents_false_shutdown_quiescence() {
    for (create, delete) in [(1, 0), (0, 1)] {
        let combined = combine(
            &CreateTopicsProgress {
                unsettled: create,
                driver_progress: false,
                next_deadline: Some(Deadline::from_tick(9)),
            },
            &DeleteTopicsProgress {
                unsettled: delete,
                driver_progress: false,
                next_deadline: Some(Deadline::from_tick(5)),
            },
        );
        assert_ne!(combined.unsettled, 0);
        assert_eq!(combined.next_deadline, Some(Deadline::from_tick(5)));
    }
}

#[test]
fn delete_timeout_uses_time_recaptured_after_create_work() {
    let observed = Cell::new(Moment::from_tick(1_000_000));
    let create_now = observed.get();
    let create_progress = CreateTopicsProgress {
        unsettled: 1,
        driver_progress: true,
        next_deadline: None,
    };
    let result = drive_create_then_capture_delete(
        create_now,
        |now| {
            assert_eq!(now, Moment::from_tick(1_000_000));
            observed.set(Moment::from_tick(4_000_000));
            Ok(create_progress)
        },
        || Ok(observed.get()),
    );
    let Ok((_create, delete_now)) = result else {
        panic!("deterministic turn moments should remain representable");
    };

    assert_eq!(
        remaining_timeout_ms(delete_now, Deadline::from_tick(11_000_000)),
        Ok(7)
    );
}
