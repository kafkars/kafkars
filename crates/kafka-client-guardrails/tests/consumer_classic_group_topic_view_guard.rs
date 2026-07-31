//! Exact client adapter ownership for the driver's immutable topic view.

mod support;

use support::{
    AuthorityToken, CapabilityAllow, CapabilityRule, LinearOwner, authority_token_violations,
    capability_violations, fixture_files, linear_violations, load_config, workspace_root,
};

const ENGINE_ROOT: &str = "crates/kafka-client-engine/src";
const TOPIC_VIEW_ADAPTER: &str =
    "crates/kafka-client-engine/src/driver/rpc/topic_view/partition_count.rs";
const PRODUCER_TOPIC_VIEW_ADAPTER: &str =
    "crates/kafka-client-engine/src/driver/rpc/topic_view/producer.rs";
const FETCH_TOPIC_VIEW_ADAPTER: &str = "crates/kafka-client-engine/src/driver/rpc/fetch/route.rs";
const TOPIC_VIEW_ADAPTERS: &[&str] = &[
    TOPIC_VIEW_ADAPTER,
    PRODUCER_TOPIC_VIEW_ADAPTER,
    FETCH_TOPIC_VIEW_ADAPTER,
];
const TOPIC_VIEW_CAPABILITY: &str = "kafka_driver::TopicView";
const CALL_OWNER: &str = "TopicPartitionCountCall";
const CALL_FIELDS: &[&str] = &["topic_view_topic", "topic_view_driver_call"];

#[test]
fn checked_in_topic_view_call_is_linear_private_and_mirrored() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == CALL_OWNER)
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, TOPIC_VIEW_ADAPTER);
    let authority = config
        .authority_tokens
        .iter()
        .filter(|rule| rule.owner_type == CALL_OWNER)
        .collect::<Vec<_>>();
    assert_eq!(authority.len(), 1);
    assert_eq!(authority[0].path, TOPIC_VIEW_ADAPTER);
    assert_eq!(authority[0].fields, CALL_FIELDS);
    assert_eq!(authority[0].allowed_paths, [TOPIC_VIEW_ADAPTER]);
    let mirrors = config
        .test_mirrors
        .iter()
        .filter(|rule| rule.production == TOPIC_VIEW_ADAPTER)
        .collect::<Vec<_>>();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(
        mirrors[0].test,
        "crates/kafka-client-engine/src/driver/rpc/topic_view/partition_count_test.rs"
    );
}

#[test]
fn live_topic_view_import_stays_in_the_exact_driver_rpc_adapters() {
    let workspace = workspace_root();
    let allow = TOPIC_VIEW_ADAPTERS
        .iter()
        .map(|path| topic_view_allow(path))
        .collect();
    let violations = capability_violations(
        &workspace,
        &[CapabilityRule {
            root: ENGINE_ROOT.into(),
            forbidden: vec![TOPIC_VIEW_CAPABILITY.into()],
            allow,
        }],
    );

    assert!(
        violations.is_empty(),
        "driver TopicView escaped its exact adapters:\n{}",
        violations.join("\n")
    );
}

#[test]
fn fixture_rejects_topic_view_import_beside_the_exact_adapters() {
    let (root, _) = fixture_files("consumer_classic_group_topic_view");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: vec![TOPIC_VIEW_CAPABILITY.into()],
            allow: vec![topic_view_allow(
                "src/driver/rpc/topic_view/partition_count.rs",
            )],
        }],
    );

    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("driver/rpc/topic_view/partition_count.rs")),
        "exact TopicView adapter was rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("consumer/group/intruder.rs")),
        "foreign TopicView import escaped detection: {violations:?}"
    );
}

#[test]
fn fixture_rejects_cloneable_or_foreignly_forged_topic_view_call() {
    let (root, files) = fixture_files("consumer_classic_group_topic_view");
    let linear = linear_violations(
        &root,
        &files,
        &[LinearOwner {
            owner_type: CALL_OWNER.into(),
            path: "src/linear_intruder.rs".into(),
        }],
    );
    for derived in ["derives Clone", "derives Copy"] {
        assert!(linear.iter().any(|violation| violation.contains(derived)));
    }
    let authority = authority_token_violations(
        &root,
        &files,
        &[AuthorityToken {
            owner_type: CALL_OWNER.into(),
            path: "src/driver/rpc/topic_view/partition_count.rs".into(),
            fields: CALL_FIELDS.iter().map(|field| (*field).into()).collect(),
            allowed_paths: vec!["src/driver/rpc/topic_view/partition_count.rs".into()],
        }],
    );
    assert!(authority.iter().any(|violation| {
        violation.contains("authority_intruder.rs") && violation.contains("constructs authority")
    }));
}

fn topic_view_allow(path: &str) -> CapabilityAllow {
    CapabilityAllow {
        path: path.into(),
        capability: TOPIC_VIEW_CAPABILITY.into(),
        reason:
            "This exact RPC adapter reduces the driver-owned immutable view to client policy facts."
                .into(),
    }
}
