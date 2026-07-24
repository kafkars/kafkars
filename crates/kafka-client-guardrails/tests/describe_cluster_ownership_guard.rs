//! Negative evidence for `DescribeCluster` mutation and linear ownership.

mod support;

use support::{LinearOwner, MutationOwner, fixture_files, linear_violations, mutation_violations};

#[test]
fn describe_cluster_mutation_fixture_rejects_every_registered_field() {
    let (root, files) = fixture_files("describe_cluster_ownership");
    let rules = [
        mutation_rule("DescribeClusterMachine", "state"),
        mutation_rule("DescribeClusterHost", "operations"),
        mutation_rule("DescribeClusterHost", "completions"),
        mutation_rule("DescribeClusterHost", "retained_bytes"),
        mutation_rule("DescribeClusterHost", "published_bytes"),
        mutation_rule("DescribeClusterHost", "next_operation_id"),
        mutation_rule("DescribeClusterCalls", "calls"),
        mutation_rule("DescribeClusterCalls", "settled"),
    ];
    let violations = mutation_violations(&root, &files, &rules);

    for rule in rules {
        assert!(
            violations.iter().any(|value| {
                value.contains("mutation_intruder.rs")
                    && value.contains(&rule.owner_type)
                    && value.contains(&rule.field)
            }),
            "mutation detector missed {}.{}: {violations:?}",
            rule.owner_type,
            rule.field
        );
    }
}

fn mutation_rule(owner_type: &str, field: &str) -> MutationOwner {
    MutationOwner {
        owner_type: owner_type.into(),
        field: field.into(),
        allowed_paths: vec!["src/mutation_owner.rs".into()],
    }
}

#[test]
fn describe_cluster_linear_fixture_rejects_clone_and_copy_for_every_owner() {
    let (root, files) = fixture_files("describe_cluster_ownership");
    let owner_types = [
        "DescribeClusterMachine",
        "DescribeClusterHost",
        "DescribeClusterOperation",
        "DescribeClusterShardOwner",
        "DescribeClusterObserver",
        "DescribeClusterCalls",
        "DescribeClusterCall",
        "DescribeClusterCallPermit",
        "SettledDescribeClusterCall",
    ];
    let rules = owner_types.map(|owner_type| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &rules);

    for owner_type in owner_types {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(
                violations
                    .iter()
                    .any(|value| value.contains(owner_type) && value.contains(derived)),
                "linear detector missed {derived} for {owner_type}: {violations:?}"
            );
        }
    }
}
