//! User correlation, canonicalization, and malformed-response scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, DescribeUserScramCredentialsBatch,
    DescribeUserScramCredentialsBrokerError, DescribeUserScramCredentialsEffect,
    DescribeUserScramCredentialsFailureKind, DescribeUserScramCredentialsInput,
    DescribeUserScramCredentialsMachine, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsTerminal, DescribeUserScramCredentialsUserOutcome,
    DescribeUserScramCredentialsUserResult, ScramCredentialInfo,
};

#[test]
fn filtered_results_restore_caller_order_and_sort_mechanisms() {
    let terminal = effect(
        &mut submitted(Some(vec!["zed", "alice"])),
        response(vec![
            described("alice", &[(2, 8192), (1, 4096)]),
            described("zed", &[(2, 4096)]),
        ]),
    );
    let DescribeUserScramCredentialsEffect::Complete {
        terminal: DescribeUserScramCredentialsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };

    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(DescribeUserScramCredentialsUserOutcome::user)
            .collect::<Vec<_>>(),
        ["zed", "alice"]
    );
    let DescribeUserScramCredentialsUserResult::Described(credentials) =
        batch.outcomes()[1].result()
    else {
        panic!("credential metadata expected");
    };
    assert_eq!(
        credentials
            .iter()
            .map(|info| info.mechanism())
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn all_user_results_are_sorted_by_utf8_bytes_and_may_be_empty() {
    let terminal = effect(
        &mut submitted(None),
        response(vec![
            described("zed", &[(1, 4096)]),
            described("alice", &[(2, 8192)]),
        ]),
    );
    let DescribeUserScramCredentialsEffect::Complete {
        terminal: DescribeUserScramCredentialsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(DescribeUserScramCredentialsUserOutcome::user)
            .collect::<Vec<_>>(),
        ["alice", "zed"]
    );

    assert!(matches!(
        effect(&mut submitted(None), response(Vec::new())),
        DescribeUserScramCredentialsEffect::Complete {
            terminal: DescribeUserScramCredentialsTerminal::Described(_),
            ..
        }
    ));
}

#[test]
fn exact_per_user_broker_errors_remain_correlated() {
    let error = DescribeUserScramCredentialsBrokerError::new(
        NonZeroI16::new(-29).unwrap_or_else(|| panic!("nonzero")),
        Some("unknown user".to_owned()),
        false,
    );
    let terminal = effect(
        &mut submitted(Some(vec!["alice"])),
        response(vec![
            DescribeUserScramCredentialsUserOutcome::broker_failed("alice".to_owned(), error),
        ]),
    );
    let DescribeUserScramCredentialsEffect::Complete {
        terminal: DescribeUserScramCredentialsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };
    assert!(matches!(
        batch.outcomes()[0].result(),
        DescribeUserScramCredentialsUserResult::BrokerFailed(error)
            if error.code() == -29 && error.message() == Some("unknown user")
    ));
}

#[test]
fn missing_extra_and_duplicate_users_are_invalid() {
    let malformed = [
        vec![described("zed", &[(1, 4096)])],
        vec![
            described("zed", &[(1, 4096)]),
            described("other", &[(2, 4096)]),
        ],
        vec![
            described("zed", &[(1, 4096)]),
            described("zed", &[(2, 4096)]),
        ],
    ];
    for outcomes in malformed {
        assert_invalid(
            &mut submitted(Some(vec!["zed", "alice"])),
            response(outcomes),
        );
    }
}

#[test]
fn malformed_credential_metadata_is_invalid_without_partial_success() {
    let malformed = [
        described("", &[(1, 4096)]),
        described("alice", &[]),
        described("alice", &[(0, 4096)]),
        described("alice", &[(-1, 4096)]),
        described("alice", &[(1, 0)]),
        described("alice", &[(1, 4096), (1, 8192)]),
    ];
    for outcome in malformed {
        assert_invalid(&mut submitted(None), response(vec![outcome]));
    }
}

#[test]
fn oversized_per_user_diagnostic_is_invalid() {
    let error = DescribeUserScramCredentialsBrokerError::new(
        NonZeroI16::new(29).unwrap_or_else(|| panic!("nonzero")),
        Some("x".repeat(DESCRIBE_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES + 1)),
        false,
    );
    assert_invalid(
        &mut submitted(None),
        response(vec![
            DescribeUserScramCredentialsUserOutcome::broker_failed("alice".to_owned(), error),
        ]),
    );
}

fn submitted(users: Option<Vec<&str>>) -> DescribeUserScramCredentialsMachine {
    let users = users.map(|users| users.into_iter().map(str::to_owned).collect());
    let plan = DescribeUserScramCredentialsPlan::new(users)
        .unwrap_or_else(|error| panic!("valid selection: {error}"));
    let mut machine = DescribeUserScramCredentialsMachine::new(
        OperationId::from_raw(50),
        Deadline::from_tick(100),
        plan,
    );
    effect(
        &mut machine,
        DescribeUserScramCredentialsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(DescribeUserScramCredentialsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver accepted: {error}"));
    machine
}

fn described(user: &str, credentials: &[(i8, u32)]) -> DescribeUserScramCredentialsUserOutcome {
    DescribeUserScramCredentialsUserOutcome::described(
        user.to_owned(),
        credentials
            .iter()
            .map(|&(mechanism, iterations)| ScramCredentialInfo::new(mechanism, iterations))
            .collect(),
    )
}

fn response(
    outcomes: Vec<DescribeUserScramCredentialsUserOutcome>,
) -> DescribeUserScramCredentialsInput {
    DescribeUserScramCredentialsInput::BrokerResponded {
        batch: DescribeUserScramCredentialsBatch::new(7, outcomes),
    }
}

fn effect(
    machine: &mut DescribeUserScramCredentialsMachine,
    input: DescribeUserScramCredentialsInput,
) -> DescribeUserScramCredentialsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn assert_invalid(
    machine: &mut DescribeUserScramCredentialsMachine,
    input: DescribeUserScramCredentialsInput,
) {
    let DescribeUserScramCredentialsEffect::Complete {
        terminal: DescribeUserScramCredentialsTerminal::Failed(failure),
        ..
    } = effect(machine, input)
    else {
        panic!("failed terminal expected");
    };
    assert_eq!(
        failure.kind(),
        &DescribeUserScramCredentialsFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
