//! The checked-in policy uses a strict, versioned schema.

mod support;

use support::{load_config, parse_config, read, workspace_root};

#[test]
fn checked_in_policy_is_supported_and_internally_ordered() {
    let workspace = workspace_root();
    let config = load_config(&workspace);

    assert!(
        !config.forbidden_transitive_dependencies.is_empty(),
        "dependency denylist must name at least one reviewed runtime"
    );
    assert!(
        config
            .forbidden_transitive_dependencies
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "dependency denylist must be strictly ordered and duplicate-free"
    );
    for budget in [
        config.budgets.facade,
        config.budgets.implementation,
        config.budgets.test,
        config.budgets.auxiliary,
    ] {
        assert!(
            budget.target <= budget.soft && budget.soft <= budget.hard,
            "file budgets must satisfy target <= soft <= hard"
        );
    }
}

#[test]
fn unknown_policy_keys_are_rejected() {
    let workspace = workspace_root();
    let source = read(&workspace.join("guardrails.toml"));
    let invalid = source.replacen("schema = 3", "schema = 3\nunknown_policy_key = true", 1);

    assert!(
        parse_config(&invalid).is_err(),
        "strict policy parser accepted an unknown key"
    );
}
