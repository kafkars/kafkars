//! Exact engine exceptions required by hosted checkpoint commit observation.

use super::support::{load_config, workspace_root};

const ENGINE_CHANNEL_REASON: &str = "A test-only channel records one off-thread wake so Future \
    observation can be proven against the same recovered terminal as blocking wait.";
const INVALIDATE_REASON: &str = "A correlated coordinator rejection consumes its retained \
    route-failure token through the driver-owned causal invalidation barrier before terminal \
    publication.";

#[test]
fn engine_channel_invalidation_and_facade_baselines_are_exact() {
    let config = load_config(&workspace_root());
    for (path, lines) in [
        ("crates/kafka-client-engine/src/consumer/group.rs", 389),
        ("crates/kafka-client-engine/src/consumer/mod.rs", 98),
    ] {
        let baselines = config
            .budgets
            .baseline
            .iter()
            .filter(|baseline| baseline.path == path)
            .collect::<Vec<_>>();
        assert_eq!(baselines.len(), 1, "{path} needs one exact baseline");
        assert_eq!(baselines[0].lines, lines);
    }

    let engine = config
        .capability_rules
        .iter()
        .find(|rule| rule.root == "crates/kafka-client-engine/src")
        .unwrap_or_else(|| panic!("engine capability root"));
    let channel = engine
        .allow
        .iter()
        .filter(|allow| {
            allow.path == "crates/kafka-client-engine/src/consumer/group_commit/observer_test.rs"
                && allow.capability == "std::sync::mpsc::channel"
        })
        .collect::<Vec<_>>();
    assert_eq!(channel.len(), 1);
    assert_eq!(channel[0].reason, ENGINE_CHANNEL_REASON);

    let settlement = config
        .capability_rules
        .iter()
        .find(|rule| {
            rule.root
                == "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_settlement.rs"
        })
        .unwrap_or_else(|| panic!("group commit settlement capability root"));
    assert!(
        settlement
            .forbidden
            .iter()
            .any(|value| value == "invalidate")
    );
    assert_eq!(settlement.allow.len(), 1);
    assert_eq!(
        settlement.allow[0].path,
        "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_settlement.rs"
    );
    assert_eq!(settlement.allow[0].capability, "invalidate");
    assert_eq!(settlement.allow[0].reason, INVALIDATE_REASON);

    let invalidate = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "invalidate")
        .collect::<Vec<_>>();
    assert_eq!(invalidate.len(), 1);
    assert_eq!(invalidate[0].root, "crates/kafka-client-engine/src");
    assert_eq!(
        invalidate[0].allowed_paths,
        [
            "crates/kafka-client-engine/src/driver/rpc/classic_group/coordinator_invalidation_drive.rs",
            "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_settlement.rs",
            "crates/kafka-client-engine/src/driver/rpc/reassignment_controller_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/transaction_control/add_partitions_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/transaction_control/end.rs",
            "crates/kafka-client-engine/src/driver/rpc/transaction_offsets/add_offsets_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/transaction_offsets/offset_commit_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/transaction_produce/route_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/add_raft_voter_terminal/refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/calls/route_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/create_partitions_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/delete_topics_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/elect_leaders_terminal.rs",
            "crates/kafka-client-engine/src/driver/rpc/fetch/route_refresh.rs",
            "crates/kafka-client-engine/src/driver/rpc/remove_raft_voter_terminal.rs",
            "crates/kafka-client-engine/src/driver/rpc/transaction_init_call.rs",
            "crates/kafka-client-engine/src/driver/rpc/unregister_broker_terminal.rs",
            "crates/kafka-client-engine/src/driver/rpc/update_features_terminal/refresh.rs",
        ]
    );
}
