//! Caller-ordered partial-result settlement after accepted work fails.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual transition failures"
)]

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminClassicConsumerGroupDetails, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupDescriptionResult, AdminDescribeConsumerGroupsEffect,
    AdminDescribeConsumerGroupsFailureKind, AdminDescribeConsumerGroupsInput,
    AdminDescribeConsumerGroupsMachine, AdminDescribeConsumerGroupsPlan,
    AdminDescribeConsumerGroupsTerminal,
};

#[test]
fn later_pre_driver_rejection_preserves_success_and_marks_unattempted_groups() {
    let mut machine = started_after_first_success();
    let terminal = machine
        .apply(AdminDescribeConsumerGroupsInput::DriverRejected)
        .unwrap_or_else(|error| panic!("reject second group: {error}"))
        .into_effect();
    assert_partial(
        terminal,
        AdminDescribeConsumerGroupsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn later_transport_failure_preserves_success_and_marks_unattempted_groups() {
    let mut machine = started_after_first_success();
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second group: {error}"));
    let terminal = machine
        .apply(AdminDescribeConsumerGroupsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("fail second group: {error}"))
        .into_effect();
    assert_partial(
        terminal,
        AdminDescribeConsumerGroupsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

fn started_after_first_success() -> AdminDescribeConsumerGroupsMachine {
    let mut machine = AdminDescribeConsumerGroupsMachine::new(
        OperationId::from_raw(19),
        Deadline::from_tick(100),
        AdminDescribeConsumerGroupsPlan::new(
            vec!["alpha".to_owned(), "beta".to_owned(), "gamma".to_owned()],
            false,
        )
        .expect("valid plan"),
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AdminDescribeConsumerGroupsInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
                throttle_time_ms: 7,
                outcome: described("alpha"),
            })
        })
        .unwrap_or_else(|error| panic!("complete first group: {error}"));
    machine
}

fn described(group_id: &str) -> AdminConsumerGroupDescriptionOutcome {
    AdminConsumerGroupDescriptionOutcome::described(
        group_id.to_owned(),
        AdminConsumerGroupDescription::new(
            "Stable".to_owned(),
            AdminConsumerGroupDescriptionDetails::Classic(AdminClassicConsumerGroupDetails::new(
                "consumer".to_owned(),
                "range".to_owned(),
            )),
            Vec::new(),
            None,
        ),
    )
}

fn assert_partial(
    effect: Option<AdminDescribeConsumerGroupsEffect>,
    expected_kind: AdminDescribeConsumerGroupsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let Some(AdminDescribeConsumerGroupsEffect::Complete {
        terminal: AdminDescribeConsumerGroupsTerminal::Described(batch),
        ..
    }) = effect
    else {
        panic!("missing partial-result terminal");
    };
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 7);
    assert_eq!(outcomes.len(), 3);
    let mut outcomes = outcomes.into_iter();
    let (first_id, first) = outcomes.next().expect("first").into_parts();
    assert_eq!(first_id, "alpha");
    assert!(matches!(
        first,
        AdminConsumerGroupDescriptionResult::Described(_)
    ));
    let (second_id, second) = outcomes.next().expect("second").into_parts();
    assert_eq!(second_id, "beta");
    let AdminConsumerGroupDescriptionResult::OperationFailed(second) = second else {
        panic!("current group did not retain its operation failure");
    };
    assert_eq!(second.kind(), expected_kind);
    assert_eq!(second.delivery(), expected_delivery);
    let (third_id, third) = outcomes.next().expect("third").into_parts();
    assert_eq!(third_id, "gamma");
    let AdminConsumerGroupDescriptionResult::OperationFailed(third) = third else {
        panic!("unattempted group did not retain its operation failure");
    };
    assert_eq!(
        third.kind(),
        AdminDescribeConsumerGroupsFailureKind::NotAttempted
    );
    assert_eq!(third.delivery(), DeliveryStatus::NotSent);
}
