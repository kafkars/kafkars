//! Retrying an unsent creation reuses the original admission and absolute deadline.

use std::time::{Duration, Instant};

use kafka_client_core::{
    CreateTopicSpecification, CreateTopicsInput, CreateTopicsPlan, Deadline, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    CreateTopicsOutcome, CreateTopicsTurn,
    test_support::{create_topics_host, stop_notifier},
};

#[test]
fn controller_retry_keeps_one_terminal_reservation_and_both_deadlines() {
    let (mut host, notifier) = create_topics_host();
    let plan = CreateTopicsPlan::new(
        vec![CreateTopicSpecification::new("orders", 3, 2, Vec::new())],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(5),
    );
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan.clone(), 16 * 1024)
        .unwrap_or_else(|error| panic!("admit creation: {error:?}"));
    let CreateTopicsTurn::Submit(first) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take first attempt: {error}"))
    else {
        panic!("first submission required");
    };
    host.apply(first.operation_id, CreateTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first attempt: {error}"));
    host.apply(
        first.operation_id,
        CreateTopicsInput::ControllerRouteUnavailable {
            now: Moment::from_tick(3),
        },
    )
    .unwrap_or_else(|error| panic!("authorize retry: {error}"));
    assert_eq!(host.unsettled(), 1);
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);
    let CreateTopicsTurn::Submit(retry) = host
        .turn(Moment::from_tick(4))
        .unwrap_or_else(|error| panic!("take retry: {error}"))
    else {
        panic!("one replacement submission required");
    };
    assert_eq!(retry.operation_id, first.operation_id);
    assert_eq!(retry.deadline, deadline);
    assert_eq!(retry.plan, plan);
    assert_eq!(retry.retained_bytes, first.retained_bytes);
    host.apply(retry.operation_id, CreateTopicsInput::DriverRejected)
        .unwrap_or_else(|error| panic!("settle unadmitted retry: {error}"));
    assert!(matches!(
        admission.observer.wait(),
        Ok(CreateTopicsOutcome::Failed(_))
    ));
    host.turn(Moment::from_tick(5))
        .unwrap_or_else(|error| panic!("reclaim completion: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(notifier);
}
