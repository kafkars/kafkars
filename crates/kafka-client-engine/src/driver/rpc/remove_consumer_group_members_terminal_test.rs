//! Selected-version and driver-authoritative failure classification scenarios.

use kafka_client_core::{
    ConsumerGroupMemberRemoval, DeliveryStatus, RemoveConsumerGroupMembersPlan,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::LeaveGroupResponse;
use kafka_wire_core::DecodeError;

use super::remove_consumer_group_members_terminal::{
    RecoveredRemoveConsumerGroupMembersCall, RemoveConsumerGroupMembersDriverFailureKind,
    RemoveConsumerGroupMembersTerminalFact, retain_remove_consumer_group_members_terminal,
};

#[test]
fn response_fact_borrows_selected_version_and_generated_response() {
    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 19;
    let plan = plan("workers", &["instance-a", "instance-b"], Some("drain"));
    let terminal = retain_remove_consumer_group_members_terminal(
        plan.clone(),
        4_096,
        8_192,
        Some(ApiVersion::new(5)),
        Ok(response),
        None,
    );
    let RemoveConsumerGroupMembersTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("generated response expected");
    };
    assert_eq!(selected_version, Some(5));
    assert_eq!(response.throttle_time_ms, 19);
    assert!(terminal.matches(&plan, 4_096, 8_192));
    assert!(!terminal.matches(&plan, 4_095, 8_192));
    assert!(!terminal.matches(&plan, 4_096, 8_191));
    terminal.discard();
}

#[test]
fn failures_preserve_delivery_certainty_and_stable_classification() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            RemoveConsumerGroupMembersDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(13),
                minimum: ApiVersion::new(5),
                negotiated_maximum: ApiVersion::new(4),
            },
            RemoveConsumerGroupMembersDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            RemoveConsumerGroupMembersDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            RemoveConsumerGroupMembersDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_remove_consumer_group_members_terminal(
            plan("workers", &["instance-a"], None),
            4_096,
            8_192,
            None,
            Err(error),
            None,
        );
        let RemoveConsumerGroupMembersTerminalFact::Failed { kind, delivery } = terminal.fact()
        else {
            panic!("failure fact expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn correlation_rejects_group_member_order_and_reason_mismatches() {
    let expected = plan("workers", &["instance-a", "instance-b"], Some("drain"));
    let terminal = retain_remove_consumer_group_members_terminal(
        expected.clone(),
        4_096,
        8_192,
        Some(ApiVersion::new(5)),
        Ok(LeaveGroupResponse::default()),
        None,
    );

    assert!(!terminal.matches(
        &plan(
            "other-workers",
            &["instance-a", "instance-b"],
            Some("drain")
        ),
        4_096,
        8_192,
    ));
    assert!(!terminal.matches(
        &plan("workers", &["instance-b", "instance-a"], Some("drain")),
        4_096,
        8_192,
    ));
    assert!(!terminal.matches(
        &plan("workers", &["instance-a", "instance-b"], Some("replace")),
        4_096,
        8_192,
    ));
    terminal.discard();
}

#[test]
fn exact_shutdown_recovery_token_seals_linearly() {
    let plan = plan("workers", &["instance-a"], Some("drain"));
    let recovered = RecoveredRemoveConsumerGroupMembersCall::new(plan.clone(), 4_096, 8_192);
    assert!(recovered.matches(&plan, 4_096, 8_192));
    recovered.seal();
}

fn plan(group_id: &str, members: &[&str], reason: Option<&str>) -> RemoveConsumerGroupMembersPlan {
    RemoveConsumerGroupMembersPlan::new(
        group_id.to_owned(),
        members
            .iter()
            .map(|member| ConsumerGroupMemberRemoval::new((*member).to_owned()))
            .collect(),
        reason.map(str::to_owned),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
