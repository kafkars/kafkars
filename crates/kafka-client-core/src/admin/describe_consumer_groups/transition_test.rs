//! Sequential coordinator-call lifecycle scenarios.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual transition failures"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminClassicConsumerGroupDetails, AdminConsumerGroupBrokerError, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionOutcome,
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsFailureKind,
    AdminDescribeConsumerGroupsInput, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsTerminal,
};

#[test]
fn caller_order_crosses_sequential_singleton_calls_under_one_deadline() {
    let mut machine = machine();
    let first = machine
        .apply(AdminDescribeConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"))
        .into_effect();
    assert!(matches!(
        first,
        Some(AdminDescribeConsumerGroupsEffect::Submit {
            group_id,
            deadline,
            ..
        }) if group_id == "beta" && deadline == Deadline::from_tick(50)
    ));
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("first accept: {error}"));
    let second = machine
        .apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 7,
            outcome: AdminConsumerGroupDescriptionOutcome::described(
                "beta".to_owned(),
                AdminConsumerGroupDescription::new(
                    "Stable".to_owned(),
                    AdminConsumerGroupDescriptionDetails::Classic(
                        AdminClassicConsumerGroupDetails::new(
                            "consumer".to_owned(),
                            "range".to_owned(),
                        ),
                    ),
                    Vec::new(),
                    None,
                ),
            ),
        })
        .unwrap_or_else(|error| panic!("first response: {error}"))
        .into_effect();
    assert!(matches!(
        second,
        Some(AdminDescribeConsumerGroupsEffect::Submit { group_id, .. })
            if group_id == "alpha"
    ));
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("second accept: {error}"));
    let terminal = machine
        .apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcome: AdminConsumerGroupDescriptionOutcome::broker_failed(
                "alpha".to_owned(),
                AdminConsumerGroupBrokerError::new(
                    NonZeroI16::new(69).expect("nonzero"),
                    None,
                    false,
                ),
            ),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"))
        .into_effect();
    let Some(AdminDescribeConsumerGroupsEffect::Complete {
        terminal: AdminDescribeConsumerGroupsTerminal::Described(batch),
        ..
    }) = terminal
    else {
        panic!("missing batch terminal");
    };
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 11);
    assert_eq!(outcomes[0].group_id(), "beta");
    assert_eq!(outcomes[1].group_id(), "alpha");
}

#[test]
fn later_definite_rejection_is_not_sent_for_the_current_group() {
    let mut machine = machine();
    machine
        .apply(AdminDescribeConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AdminDescribeConsumerGroupsInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
                throttle_time_ms: 0,
                outcome: AdminConsumerGroupDescriptionOutcome::described(
                    "beta".to_owned(),
                    AdminConsumerGroupDescription::new(
                        String::new(),
                        AdminConsumerGroupDescriptionDetails::Classic(
                            AdminClassicConsumerGroupDetails::new(String::new(), String::new()),
                        ),
                        Vec::new(),
                        None,
                    ),
                ),
            })
        })
        .unwrap_or_else(|error| panic!("first group: {error}"));
    let terminal = machine
        .apply(AdminDescribeConsumerGroupsInput::DriverRejected)
        .unwrap_or_else(|error| panic!("rejection: {error}"))
        .into_effect();
    let Some(AdminDescribeConsumerGroupsEffect::Complete {
        terminal: AdminDescribeConsumerGroupsTerminal::Described(batch),
        ..
    }) = terminal
    else {
        panic!("missing partial-result terminal");
    };
    let (_, outcomes) = batch.into_parts();
    let (_, result) = outcomes
        .into_iter()
        .nth(1)
        .expect("second group")
        .into_parts();
    let super::AdminConsumerGroupDescriptionResult::OperationFailed(failure) = result else {
        panic!("second group was not an operation failure");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeConsumerGroupsFailureKind::DriverRejected
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn machine() -> AdminDescribeConsumerGroupsMachine {
    AdminDescribeConsumerGroupsMachine::new(
        OperationId::from_raw(8),
        Deadline::from_tick(50),
        AdminDescribeConsumerGroupsPlan::new(vec!["beta".to_owned(), "alpha".to_owned()], false)
            .expect("valid plan"),
    )
}
