//! Non-secret plan validation scenarios for SCRAM credential alteration.

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES, ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS,
    ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES, ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS,
    ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS, ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    ALTER_USER_SCRAM_CREDENTIALS_SHA_512, AlterUserScramCredentialChange,
    AlterUserScramCredentialChangeKind, AlterUserScramCredentialsPlan,
    AlterUserScramCredentialsPlanError,
};

#[test]
fn plan_retains_change_order_and_derives_first_user_occurrences() {
    let plan = AlterUserScramCredentialsPlan::new(vec![
        AlterUserScramCredentialChange::upsertion(
            "bob".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
            ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS,
        ),
        AlterUserScramCredentialChange::deletion(
            "alice".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
        ),
        AlterUserScramCredentialChange::deletion(
            "bob".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
        ),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.changes()[0].user(), "bob");
    assert_eq!(
        plan.changes()[0].kind(),
        AlterUserScramCredentialChangeKind::Upsertion {
            iterations: ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS,
        }
    );
    assert_eq!(
        plan.affected_users(),
        &["bob".to_owned(), "alice".to_owned()]
    );
}

#[test]
fn empty_names_unknown_mechanisms_and_invalid_iterations_are_rejected() {
    for (change, expected) in [
        (
            AlterUserScramCredentialChange::deletion(
                String::new(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
            ),
            AlterUserScramCredentialsPlanError::EmptyUserName,
        ),
        (
            AlterUserScramCredentialChange::deletion("alice".to_owned(), 0),
            AlterUserScramCredentialsPlanError::UnknownMechanism,
        ),
        (
            AlterUserScramCredentialChange::deletion("alice".to_owned(), 3),
            AlterUserScramCredentialsPlanError::UnknownMechanism,
        ),
        (
            AlterUserScramCredentialChange::upsertion(
                "alice".to_owned(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
                ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS - 1,
            ),
            AlterUserScramCredentialsPlanError::IterationsOutOfRange,
        ),
        (
            AlterUserScramCredentialChange::upsertion(
                "alice".to_owned(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
                ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS + 1,
            ),
            AlterUserScramCredentialsPlanError::IterationsOutOfRange,
        ),
    ] {
        assert_eq!(
            AlterUserScramCredentialsPlan::new(vec![change]),
            Err(expected)
        );
    }
}

#[test]
fn iteration_domain_is_inclusive_and_does_not_apply_to_deletions() {
    for iterations in [
        ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS,
        ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS,
    ] {
        AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::upsertion(
            "alice".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
            iterations,
        )])
        .unwrap_or_else(|error| panic!("boundary iteration should be valid: {error}"));
    }
    AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
        "alice".to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
    )])
    .unwrap_or_else(|error| panic!("deletion has no iteration count: {error}"));
}

#[test]
fn duplicate_user_mechanism_is_rejected_but_two_mechanisms_are_distinct() {
    let distinct = vec![
        AlterUserScramCredentialChange::deletion(
            "alice".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
        ),
        AlterUserScramCredentialChange::deletion(
            "alice".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
        ),
    ];
    AlterUserScramCredentialsPlan::new(distinct.clone())
        .unwrap_or_else(|error| panic!("two mechanisms should be distinct: {error}"));

    let mut duplicate = distinct;
    duplicate.push(AlterUserScramCredentialChange::upsertion(
        "alice".to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
        ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS,
    ));
    assert_eq!(
        AlterUserScramCredentialsPlan::new(duplicate),
        Err(AlterUserScramCredentialsPlanError::DuplicateCredential)
    );
}

#[test]
fn empty_overlong_and_over_capacity_batches_are_rejected() {
    assert_eq!(
        AlterUserScramCredentialsPlan::new(Vec::new()),
        Err(AlterUserScramCredentialsPlanError::EmptyBatch)
    );
    assert_eq!(
        AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
            "x".repeat(ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES + 1),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
        )]),
        Err(AlterUserScramCredentialsPlanError::UserNameTooLong)
    );
    assert_eq!(
        AlterUserScramCredentialsPlan::new(vec![
            AlterUserScramCredentialChange::deletion(
                "alice".to_owned(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
            );
            ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES + 1
        ]),
        Err(AlterUserScramCredentialsPlanError::TooManyChanges)
    );
}

#[test]
fn distinct_user_bound_is_never_wider_than_the_change_bound() {
    let changes = (0..=ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS)
        .map(|index| {
            AlterUserScramCredentialChange::deletion(
                format!("user-{index}"),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
            )
        })
        .collect();
    assert_eq!(
        AlterUserScramCredentialsPlan::new(changes),
        Err(AlterUserScramCredentialsPlanError::TooManyChanges)
    );
}

#[test]
fn change_consumption_exposes_only_non_secret_scalar_intent() {
    let change = AlterUserScramCredentialChange::upsertion(
        "alice".to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
        8192,
    );
    assert_eq!(
        change.into_parts(),
        (
            "alice".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
            AlterUserScramCredentialChangeKind::Upsertion { iterations: 8192 },
        )
    );
}
