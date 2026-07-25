//! Original-deadline and timeout ownership scenarios for one classic cycle.

use std::time::Duration;

use kafka_client_core::{
    ClassicGroupErrorKind, ClassicGroupPhase, ClassicGroupTiming, ClassicProtocol, GroupId,
};

use crate::clock::MonotonicClock;

use super::{
    classic_group_execution::{ClassicGroupExecutionError, new_classic_group_execution},
    classic_group_owner::ClassicGroupOwner,
};

#[test]
fn begin_retains_exact_timing_and_core_and_transport_deadline() {
    let mut owner = owner();
    let mut execution = new_classic_group_execution();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(7))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));

    let cycle = execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let prepared = execution
        .prepared_join()
        .unwrap_or_else(|| panic!("prepared Join expected"));

    assert_eq!(prepared.group_id(), owner.machine().group_id());
    assert_eq!(prepared.cycle(), cycle);
    assert_eq!(prepared.protocol(), ClassicProtocol::Range);
    assert_eq!(prepared.timing(), owner.machine().timing());
    assert_eq!(prepared.deadline(), capture.operation_deadline());
    assert_eq!(execution.next_deadline(), Some(capture.deadline()));
    assert_eq!(execution.unsettled(), 1);
}

#[test]
fn exact_deadline_expires_the_cycle_and_releases_the_join_intent() {
    let mut owner = owner();
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_nanos(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));

    assert_eq!(
        execution.expire_if_due(&mut owner, capture.now()),
        Ok(false)
    );
    assert_eq!(
        execution.expire_if_due(
            &mut owner,
            kafka_client_core::Moment::from_tick(capture.deadline().tick()),
        ),
        Ok(true)
    );
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Lost);
    assert_eq!(execution.unsettled(), 0);
    assert!(execution.prepared_join().is_none());
}

#[test]
fn a_second_begin_cannot_replace_the_retained_cycle() {
    let mut owner = owner();
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));

    assert_eq!(
        execution.begin(&mut owner, capture),
        Err(ClassicGroupExecutionError::Occupied)
    );
    assert_ne!(
        ClassicGroupExecutionError::Core(ClassicGroupErrorKind::InvalidPhase),
        ClassicGroupExecutionError::Occupied
    );
}

fn owner() -> ClassicGroupOwner {
    ClassicGroupOwner::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group identity")),
        timing(),
    )
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"))
}
