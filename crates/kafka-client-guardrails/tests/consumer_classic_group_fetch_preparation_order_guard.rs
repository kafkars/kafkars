//! Source-order ratchets for group Fetch activation and front preparation.

mod support;

use support::{read, workspace_root};

const OWNER: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner.rs";
const PREPARE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/prepare.rs";

#[test]
fn activation_and_front_interpretation_order_is_source_explicit() {
    let root = workspace_root();
    let owner = read(&root.join(OWNER));
    assert_ordered(
        &owner,
        &[
            "preflight_activation_capacity",
            "prepare_replacement",
            "install_resolved_assignment",
            "commit_event_claims",
            "transition.into_effects()",
        ],
    );
    let prepare = read(&root.join(PREPARE));
    assert_ordered(
        &prepare,
        &[
            "self.pending_fetches.len() >= self.partition_capacity",
            "FetchAttemptDeadline::capture_for_fetch",
            "copy_topic_name",
            "PreparedFetchExecution::new_retaining_attempt",
            "self.pending_fetches.push_back",
        ],
    );
    let control = prepare
        .split("fn interpret_control")
        .nth(1)
        .unwrap_or_else(|| panic!("control interpreter remains explicit"));
    assert_ordered(
        control,
        &[
            "self.fetches.observe_control",
            "self.timers.observe_control",
            "self.reconcile_pending_fetches",
            "self.events.observe_effect",
            "self.effects.pop_front",
        ],
    );
    for required in [
        "ClassicGroupFetchFront::Backpressured",
        "FetchExecutionError::ControlPending",
        "if self.is_faulted()",
    ] {
        assert!(prepare.contains(required), "preparation lost {required}");
    }
}

fn assert_ordered(source: &str, tokens: &[&str]) {
    let mut previous = 0;
    for token in tokens {
        let Some(offset) = source[previous..].find(token) else {
            panic!("source lost ordered token {token}");
        };
        let index = previous + offset;
        previous = index + token.len();
    }
}
