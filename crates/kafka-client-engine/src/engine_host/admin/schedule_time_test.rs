//! Fresh-monotonic-observation scenarios across concrete admin turns.

use std::cell::Cell;

use kafka_client_core::{Deadline, Moment};

use super::{
    create_topics::CreateTopicsProgress,
    delete_topics::DeleteTopicsProgress,
    describe_cluster::DescribeClusterProgress,
    schedule::{
        drive_create_then_capture_delete, drive_delete_then_capture_describe,
        drive_describe_then_capture_topics,
    },
};
use crate::protocol::admin::delete_topics::remaining_timeout_ms;

#[test]
fn delete_timeout_uses_time_recaptured_after_create_work() {
    let observed = Cell::new(Moment::from_tick(1_000_000));
    let create_progress = CreateTopicsProgress {
        unsettled: 1,
        driver_progress: true,
        next_deadline: None,
    };
    let result = drive_create_then_capture_delete(
        observed.get(),
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

#[test]
fn describe_cluster_uses_time_recaptured_after_delete_work() {
    let observed = Cell::new(Moment::from_tick(4_000_000));
    let delete_progress = DeleteTopicsProgress {
        unsettled: 1,
        driver_progress: true,
        next_deadline: None,
    };
    let result = drive_delete_then_capture_describe(
        observed.get(),
        |now| {
            assert_eq!(now, Moment::from_tick(4_000_000));
            observed.set(Moment::from_tick(8_000_000));
            Ok(delete_progress)
        },
        || Ok(observed.get()),
    );
    let Ok((_delete, describe_now)) = result else {
        panic!("deterministic turn moments should remain representable");
    };
    assert_eq!(describe_now, Moment::from_tick(8_000_000));
}

#[test]
fn describe_topics_uses_time_recaptured_after_describe_cluster_work() {
    let observed = Cell::new(Moment::from_tick(8_000_000));
    let describe_progress = DescribeClusterProgress {
        unsettled: 1,
        driver_progress: true,
        next_deadline: None,
    };
    let result = drive_describe_then_capture_topics(
        observed.get(),
        |now| {
            assert_eq!(now, Moment::from_tick(8_000_000));
            observed.set(Moment::from_tick(12_000_000));
            Ok(describe_progress)
        },
        || Ok(observed.get()),
    );
    let Ok((_describe, topics_now)) = result else {
        panic!("deterministic turn moments should remain representable");
    };
    assert_eq!(topics_now, Moment::from_tick(12_000_000));
}
