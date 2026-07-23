//! Producer mutation ownership and linear-lifecycle architecture checks.

mod support;

use support::{
    AuthorityToken, LinearOwner, MutationOwner, authority_linear_violations,
    authority_token_violations, fixture_files, linear_violations, load_config, mutation_violations,
    rust_files, workspace_root,
};

#[test]
fn checked_in_producer_ownership_is_narrow_and_linear() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
    let mut violations = mutation_violations(&workspace, &files, &config.mutation_owners);
    violations.extend(linear_violations(&workspace, &files, &config.linear_owners));
    violations.extend(authority_token_violations(
        &workspace,
        &files,
        &config.authority_tokens,
    ));
    violations.extend(authority_linear_violations(
        &config.authority_tokens,
        &config.linear_owners,
    ));
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn authority_fixture_rejects_construction_mutation_and_visibility() {
    let (root, files) = fixture_files("authority_ownership");
    let rules = [
        AuthorityToken {
            owner_type: "NotificationBudgetAuthority".into(),
            path: "src/owner.rs".into(),
            fields: vec![
                "budget_completion_capacity".into(),
                "budget_pending_capacity".into(),
                "budget_queue_capacity".into(),
                "_budget_proof".into(),
            ],
            allowed_paths: vec!["src/owner.rs".into()],
        },
        AuthorityToken {
            owner_type: "PendingNotificationDispatchAuthority".into(),
            path: "src/public.rs".into(),
            fields: vec!["_dispatch_proof".into()],
            allowed_paths: vec!["src/public.rs".into()],
        },
        authority_rule(
            "OmittedAuthority",
            "src/omitted.rs",
            &["expected_first", "expected_second"],
        ),
        authority_rule("ExtraAuthority", "src/extra.rs", &["expected"]),
        authority_rule("PublicAuthority", "src/public_type.rs", &["private_field"]),
        authority_rule("NonLeafAuthority", "src/non_leaf.rs", &["private_field"]),
        authority_rule("MissingAuthority", "src/wrong_type.rs", &["private_field"]),
    ];
    let violations = authority_token_violations(&root, &files, &rules);
    assert!(
        violations
            .iter()
            .any(|value| value.contains("intruder.rs") && value.contains("constructs authority"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("self_construction.rs")
                && value.contains("constructs authority"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("non-private field"))
    );
    for field in [
        "budget_completion_capacity",
        "budget_pending_capacity",
        "budget_queue_capacity",
    ] {
        assert!(
            violations
                .iter()
                .any(|value| value.contains("intruder.rs") && value.contains(field)),
            "missing mutation evidence for {field}: {violations:?}"
        );
    }
    for evidence in [
        "fields differ",
        "beyond crate visibility",
        "not a leaf authority module",
        "MissingAuthority is not declared",
        "inside a macro",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(evidence)),
            "missing authority declaration evidence {evidence}: {violations:?}"
        );
    }
}

fn authority_rule(owner_type: &str, path: &str, fields: &[&str]) -> AuthorityToken {
    AuthorityToken {
        owner_type: owner_type.into(),
        path: path.into(),
        fields: fields.iter().map(|field| (*field).into()).collect(),
        allowed_paths: vec![path.into()],
    }
}

#[test]
fn authority_fixture_rejects_clone_and_copy() {
    let (root, files) = fixture_files("authority_ownership");
    let rules = [LinearOwner {
        owner_type: "NotifierPendingDispatchOwner".into(),
        path: "src/clone.rs".into(),
    }];
    let violations = linear_violations(&root, &files, &rules);
    assert!(
        ["derives Clone", "derives Copy"]
            .into_iter()
            .all(|needle| violations.iter().any(|value| value.contains(needle)))
    );
}

#[test]
fn authority_without_matching_linear_registration_is_rejected() {
    let authorities = [authority_rule(
        "NotificationBudgetAuthority",
        "src/owner.rs",
        &[
            "budget_completion_capacity",
            "budget_pending_capacity",
            "budget_queue_capacity",
            "_budget_proof",
        ],
    )];
    let violations = authority_linear_violations(&authorities, &[]);
    assert!(
        violations
            .iter()
            .any(|value| value.contains("exactly one matching linear-owner rule"))
    );
}

#[test]
fn mutation_fixture_is_rejected() {
    let (root, files) = fixture_files("mutation_ownership");
    let rules = [
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "operations".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "queue".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "quarantine".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "generated".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
        MutationOwner {
            owner_type: "ProducerMachine".into(),
            field: "refusal".into(),
            allowed_paths: vec!["src/owner.rs".into()],
        },
    ];
    let violations = mutation_violations(&root, &files, &rules);
    assert_eq!(
        violations
            .iter()
            .filter(|value| value.contains("intruder.rs"))
            .count(),
        5
    );
}

#[test]
fn linear_owner_fixture_is_rejected() {
    let (root, files) = fixture_files("linear_ownership");
    let rules = [
        LinearOwner {
            owner_type: "CompletionLedger".into(),
            path: "src/owner.rs".into(),
        },
        LinearOwner {
            owner_type: "ProducerMachine".into(),
            path: "src/manual.rs".into(),
        },
    ];
    let violations = linear_violations(&root, &files, &rules);
    assert!(
        violations
            .iter()
            .any(|value| value.contains("derives Clone"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("derives Copy"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("manual.rs") && value.contains("manually implements Clone"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("manual.rs") && value.contains("manually implements Copy"))
    );
    assert!(violations.iter().any(
        |value| value.contains("cross_clone.rs") && value.contains("manually implements Clone")
    ));
    assert!(
        violations
            .iter()
            .any(|value| value.contains("cross_copy.rs")
                && value.contains("manually implements Copy"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("trait_alias.rs")
                && value.contains("imports or renames Clone/Copy"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("macro_impl.rs") && value.contains("opaque macro tokens"))
    );
}
