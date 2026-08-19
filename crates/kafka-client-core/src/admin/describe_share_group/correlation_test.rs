//! Scenarios for strict bounded API-77 correlation and ordering.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact terminal ownership"
)]

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeShareGroupAssignment, DescribeShareGroupDescription, DescribeShareGroupEffect,
    DescribeShareGroupFailureKind, DescribeShareGroupInput, DescribeShareGroupMachine,
    DescribeShareGroupMember, DescribeShareGroupPlan, DescribeShareGroupResult,
    DescribeShareGroupTerminal, DescribeShareGroupTopicAssignment,
};

#[test]
fn valid_description_is_canonicalized_deterministically() {
    let terminal = apply(description(
        "share-workers",
        vec![member("z", vec!["z-topic", "a-topic"]), member("a", vec![])],
    ));
    let DescribeShareGroupTerminal::Described(result) = terminal else {
        panic!("description expected");
    };
    let members = result.description().members();
    assert_eq!(members[0].member_id(), "a");
    assert_eq!(members[1].member_id(), "z");
    assert_eq!(members[1].subscribed_topic_names(), ["a-topic", "z-topic"]);
    assert_eq!(members[1].assignment().topics()[0].partitions(), [0, 2]);
}

#[test]
fn wrong_group_duplicate_member_or_unrequested_authorizations_are_invalid() {
    for description in [
        description("other", vec![member("a", vec![])]),
        description(
            "share-workers",
            vec![member("a", vec![]), member("a", vec![])],
        ),
        DescribeShareGroupDescription::new(
            "share-workers".to_owned(),
            "Stable".to_owned(),
            1,
            2,
            "uniform".to_owned(),
            Vec::new(),
            Some(3),
        ),
    ] {
        assert_failure(
            apply(description),
            DescribeShareGroupFailureKind::InvalidResponse,
        );
    }
}

#[test]
fn zero_topic_id_negative_or_duplicate_partitions_are_invalid() {
    for (topic_id, partitions) in [
        ([0; 16], vec![0]),
        ([1; 16], vec![-1]),
        ([1; 16], vec![2, 2]),
    ] {
        let member = DescribeShareGroupMember::new(
            "a".to_owned(),
            None,
            1,
            "c".to_owned(),
            "h".to_owned(),
            Vec::new(),
            DescribeShareGroupAssignment::new(vec![DescribeShareGroupTopicAssignment::new(
                topic_id,
                "orders".to_owned(),
                partitions,
            )]),
        );
        assert_failure(
            apply(description("share-workers", vec![member])),
            DescribeShareGroupFailureKind::InvalidResponse,
        );
    }
}

fn apply(description: DescribeShareGroupDescription) -> DescribeShareGroupTerminal {
    let mut machine = submitted();
    let transition = machine
        .apply(DescribeShareGroupInput::BrokerResponded {
            result: DescribeShareGroupResult::new(7, description),
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(DescribeShareGroupEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal expected");
    };
    terminal
}

fn submitted() -> DescribeShareGroupMachine {
    let mut machine = DescribeShareGroupMachine::new(
        OperationId::from_raw(77),
        Deadline::from_tick(20),
        DescribeShareGroupPlan::new("share-workers".to_owned(), false)
            .unwrap_or_else(|error| panic!("plan: {error}")),
    );
    machine
        .apply(DescribeShareGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeShareGroupInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit: {error}"));
    machine
}

fn description(
    group_id: &str,
    members: Vec<DescribeShareGroupMember>,
) -> DescribeShareGroupDescription {
    DescribeShareGroupDescription::new(
        group_id.to_owned(),
        "Stable".to_owned(),
        1,
        2,
        "uniform".to_owned(),
        members,
        None,
    )
}

fn member(member_id: &str, subscriptions: Vec<&str>) -> DescribeShareGroupMember {
    DescribeShareGroupMember::new(
        member_id.to_owned(),
        None,
        1,
        "client".to_owned(),
        "host".to_owned(),
        subscriptions.into_iter().map(str::to_owned).collect(),
        DescribeShareGroupAssignment::new(vec![DescribeShareGroupTopicAssignment::new(
            [7; 16],
            "orders".to_owned(),
            vec![2, 0],
        )]),
    )
}

fn assert_failure(terminal: DescribeShareGroupTerminal, kind: DescribeShareGroupFailureKind) {
    let DescribeShareGroupTerminal::Failed(failure) = terminal else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
