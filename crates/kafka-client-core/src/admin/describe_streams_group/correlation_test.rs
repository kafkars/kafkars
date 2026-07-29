//! Scenarios for strict bounded API-89 correlation and ordering.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupDescription, DescribeStreamsGroupEffect,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupInput, DescribeStreamsGroupMachine,
    DescribeStreamsGroupMember, DescribeStreamsGroupPlan, DescribeStreamsGroupResult,
    DescribeStreamsGroupTaskIds, DescribeStreamsGroupTerminal,
    DescribeStreamsGroupTopologyDescriptionStatus,
};

#[test]
fn valid_description_is_canonicalized_deterministically() {
    let terminal = apply(description(
        "streams-workers",
        vec![member("z", vec![2, 0]), member("a", vec![])],
        None,
    ));
    let DescribeStreamsGroupTerminal::Described(result) = terminal else {
        panic!("description expected");
    };
    let members = result.description().members();
    assert_eq!(members[0].member_id(), "a");
    assert_eq!(members[1].member_id(), "z");
    let (_, _, _, _, _, _, _, _, _, _, _, _, assignment, _, _) = members[1].clone().into_parts();
    let (active, _, _) = assignment.into_parts();
    assert_eq!(active[0].partitions(), [0, 2]);
}

#[test]
fn v0_absence_and_future_v1_status_are_preserved() {
    let v0 = apply(description("streams-workers", Vec::new(), None));
    let DescribeStreamsGroupTerminal::Described(v0) = v0 else {
        panic!("v0 result expected");
    };
    assert_eq!(v0.description().topology_description_status(), None);

    let future = apply_with_plan(
        description(
            "streams-workers",
            Vec::new(),
            Some(DescribeStreamsGroupTopologyDescriptionStatus::new(9)),
        ),
        true,
    );
    let DescribeStreamsGroupTerminal::Described(future) = future else {
        panic!("future v1 result expected");
    };
    assert_eq!(
        future
            .description()
            .topology_description_status()
            .map(DescribeStreamsGroupTopologyDescriptionStatus::raw),
        Some(9)
    );
}

#[test]
fn wrong_group_duplicate_member_or_invalid_status_pair_is_rejected() {
    for (description, topology_requested) in [
        (description("other", vec![member("a", vec![])], None), false),
        (
            description(
                "streams-workers",
                vec![member("a", vec![]), member("a", vec![])],
                None,
            ),
            false,
        ),
        (
            description(
                "streams-workers",
                Vec::new(),
                Some(DescribeStreamsGroupTopologyDescriptionStatus::new(0)),
            ),
            true,
        ),
    ] {
        assert_failure(
            apply_with_plan(description, topology_requested),
            DescribeStreamsGroupFailureKind::InvalidResponse,
        );
    }
}

#[test]
fn duplicate_or_negative_task_partitions_are_rejected() {
    for partitions in [vec![-1], vec![2, 2]] {
        assert_failure(
            apply(description(
                "streams-workers",
                vec![member("a", partitions)],
                None,
            )),
            DescribeStreamsGroupFailureKind::InvalidResponse,
        );
    }
}

fn apply(description: DescribeStreamsGroupDescription) -> DescribeStreamsGroupTerminal {
    apply_with_plan(description, false)
}

fn apply_with_plan(
    description: DescribeStreamsGroupDescription,
    topology_requested: bool,
) -> DescribeStreamsGroupTerminal {
    let mut machine = submitted(topology_requested);
    let transition = machine
        .apply(DescribeStreamsGroupInput::BrokerResponded {
            result: DescribeStreamsGroupResult::new(7, description),
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(DescribeStreamsGroupEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("terminal expected");
    };
    terminal
}

fn submitted(topology_requested: bool) -> DescribeStreamsGroupMachine {
    let mut machine = DescribeStreamsGroupMachine::new(
        OperationId::from_raw(89),
        Deadline::from_tick(20),
        DescribeStreamsGroupPlan::new("streams-workers".to_owned(), false, topology_requested)
            .unwrap_or_else(|error| panic!("plan: {error}")),
    );
    machine
        .apply(DescribeStreamsGroupInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeStreamsGroupInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit: {error}"));
    machine
}

fn description(
    group_id: &str,
    members: Vec<DescribeStreamsGroupMember>,
    status: Option<DescribeStreamsGroupTopologyDescriptionStatus>,
) -> DescribeStreamsGroupDescription {
    DescribeStreamsGroupDescription::new(
        group_id.to_owned(),
        "Stable".to_owned(),
        1,
        2,
        None,
        members,
        None,
        None,
        status,
    )
}

fn member(member_id: &str, partitions: Vec<i32>) -> DescribeStreamsGroupMember {
    let assignment = DescribeStreamsGroupAssignment::new(
        if partitions.is_empty() {
            Vec::new()
        } else {
            vec![DescribeStreamsGroupTaskIds::new(
                "subtopology".to_owned(),
                partitions,
            )]
        },
        Vec::new(),
        Vec::new(),
    );
    DescribeStreamsGroupMember::new(
        member_id.to_owned(),
        1,
        None,
        None,
        "client".to_owned(),
        "host".to_owned(),
        1,
        "process".to_owned(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        assignment,
        DescribeStreamsGroupAssignment::new(Vec::new(), Vec::new(), Vec::new()),
        false,
    )
}

fn assert_failure(terminal: DescribeStreamsGroupTerminal, kind: DescribeStreamsGroupFailureKind) {
    let DescribeStreamsGroupTerminal::Failed(failure) = terminal else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
