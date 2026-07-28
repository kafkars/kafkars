//! Join response version, role, and broker-error scenarios.

use super::{
    ClassicJoinOutcome, ClassicJoinResponseFailure,
    join_response_test_fixture::{member, response},
    normalize_classic_join_response,
    validation::RANGE_PROTOCOL,
};

#[test]
fn follower_success_is_owned_without_a_leader_member_set() {
    let joined = normalize_classic_join_response(3, &response("member-b", "member-a"))
        .unwrap_or_else(|error| panic!("Join normalization failed: {error:?}"));
    let ClassicJoinOutcome::Joined(joined) = joined else {
        panic!("joined outcome expected");
    };
    let (throttle, generation, member, role) = joined.into_parts();
    assert_eq!(throttle, 0);
    assert_eq!(generation.get(), 7);
    assert_eq!(member.as_ref(), "member-b");
    assert_eq!(role.into_leader_members(), None);
}

#[test]
fn leader_success_returns_ordered_owned_candidate_parts() {
    let mut raw = response("member-a", "member-a");
    raw.throttle_time_ms = 4;
    raw.members = vec![
        member("member-b", &["payments"]),
        member("member-a", &["orders"]),
    ];
    let joined = normalize_classic_join_response(2, &raw)
        .unwrap_or_else(|error| panic!("Join normalization failed: {error:?}"));
    let ClassicJoinOutcome::Joined(joined) = joined else {
        panic!("joined outcome expected");
    };
    let (_, _, local, role) = joined.into_parts();
    assert_eq!(local.as_ref(), "member-a");
    let members = role
        .into_leader_members()
        .unwrap_or_else(|| panic!("leader members expected"));
    let (slot, first, first_topics) = members
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("first member"))
        .into_parts();
    assert_eq!(slot.get(), 1);
    assert_eq!(first.as_ref(), "member-b");
    assert_eq!(first_topics[0].as_ref(), "payments");
}

#[test]
fn exact_versions_throttles_and_signed_broker_codes_are_preserved() {
    for version in [0, 4] {
        assert_eq!(
            normalize_classic_join_response(version, &response("a", "b")),
            Err(ClassicJoinResponseFailure::UnsupportedApiVersion(version))
        );
    }
    let mut impossible = response("a", "b");
    impossible.throttle_time_ms = 1;
    assert_eq!(
        normalize_classic_join_response(1, &impossible),
        Err(ClassicJoinResponseFailure::UnexpectedThrottleTime(1))
    );
    impossible.throttle_time_ms = -1;
    assert_eq!(
        normalize_classic_join_response(2, &impossible),
        Err(ClassicJoinResponseFailure::NegativeThrottleTime(-1))
    );
    let mut rejected = response("ignored", "ignored");
    rejected.error_code = -123;
    rejected.throttle_time_ms = 9;
    rejected.protocol_name = None;
    let normalized = normalize_classic_join_response(2, &rejected)
        .unwrap_or_else(|error| panic!("broker rejection failed: {error:?}"));
    let ClassicJoinOutcome::Rejected(rejection) = normalized else {
        panic!("broker rejection expected");
    };
    assert_eq!(rejection.error_code().get(), -123);
    assert_eq!(rejection.throttle_time_ms(), 9);
}

#[test]
fn v5_member_id_required_retains_the_broker_assigned_spelling() {
    let mut raw = response("assigned-member", "ignored");
    raw.error_code = 79;
    raw.protocol_name = None;
    let outcome = normalize_classic_join_response(5, &raw)
        .unwrap_or_else(|error| panic!("static normalization: {error:?}"));
    let ClassicJoinOutcome::MemberIdRequired { member } = outcome else {
        panic!("member-id-required outcome expected");
    };
    assert_eq!(member.as_ref(), "assigned-member");
}

#[test]
fn dynamic_member_id_required_remains_an_exact_broker_rejection() {
    for version in 1..=3 {
        let mut raw = response("", "");
        raw.error_code = 79;
        raw.protocol_name = None;
        let outcome = normalize_classic_join_response(version, &raw)
            .unwrap_or_else(|error| panic!("dynamic normalization: {error:?}"));
        let ClassicJoinOutcome::Rejected(rejection) = outcome else {
            panic!("dynamic MEMBER_ID_REQUIRED must remain rejected");
        };
        assert_eq!(rejection.error_code().get(), 79);
    }
}

#[test]
fn success_rejects_optional_protocol_inference_and_role_mismatch() {
    let mut raw = response("a", "b");
    raw.protocol_type = Some("consumer".into());
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::UnexpectedProtocolType)
    );
    raw.protocol_type = None;
    raw.protocol_name = None;
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::UnexpectedProtocolName)
    );
    raw.protocol_name = Some(RANGE_PROTOCOL.into());
    raw.members.push(member("a", &["orders"]));
    assert_eq!(
        normalize_classic_join_response(3, &raw),
        Err(ClassicJoinResponseFailure::UnexpectedFollowerMembers)
    );
}
