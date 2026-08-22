//! Independent nightly scenario sequencing with complete failure evidence.

use std::io;

use crate::real_broker_support::TestError;

use super::{
    evidence, nightly_admin, nightly_consumer, nightly_group, nightly_producer, nightly_resilience,
    nightly_resources, nightly_transaction,
};

type Scenario = (&'static str, fn() -> Result<(), TestError>);

const BEFORE_KIP_848: [Scenario; 6] = [
    (
        "producer_batching_partitioning",
        nightly_producer::batching_and_partitioning,
    ),
    (
        "producer_delivers_across_leader_movement",
        nightly_resilience::producer_delivers_across_leader_movement,
    ),
    (
        "producer_cancellation_preserves_delivery_certainty",
        nightly_producer::cancellation_preserves_delivery_certainty,
    ),
    (
        "direct_fetch_seek_offset_reset",
        nightly_consumer::fetch_seek_and_offset_reset,
    ),
    (
        "classic_join_cooperative_rebalance",
        nightly_group::classic_join_and_cooperative_rebalance,
    ),
    (
        "classic_member_shutdown_commit_resume",
        nightly_group::member_shutdown_commit_and_resume,
    ),
];

const KIP_848: Scenario = (
    "kip848_assignment_reconciliation",
    nightly_group::kip848_assignment_and_reconciliation,
);

const AFTER_KIP_848: [Scenario; 6] = [
    (
        "transaction_fencing_abort_commit_read_committed",
        nightly_transaction::fencing_abort_commit_and_read_committed,
    ),
    (
        "admin_controller_coordinator_exact_broker",
        nightly_admin::controller_coordinator_and_exact_broker,
    ),
    (
        "cluster_usable_after_broker_restart",
        nightly_resilience::cluster_usable_after_broker_restart,
    ),
    (
        "group_usable_after_coordinator_restart",
        nightly_resilience::group_usable_after_coordinator_restart,
    ),
    (
        "bounded_admission_deadlines_shutdown",
        nightly_resources::bounded_admission_deadlines_and_shutdown,
    ),
    (
        "retained_byte_recovery",
        nightly_resources::retained_byte_recovery,
    ),
];

pub(crate) fn run_nightly_matrix() -> Result<(), TestError> {
    run_matrix(true)
}

pub(crate) fn run_classic_matrix() -> Result<(), TestError> {
    run_matrix(false)
}

fn run_matrix(include_kip_848: bool) -> Result<(), TestError> {
    let mut failures = Vec::new();
    let scenarios = BEFORE_KIP_848
        .into_iter()
        .chain(include_kip_848.then_some(KIP_848))
        .chain(AFTER_KIP_848);
    for (name, scenario) in scenarios {
        if let Err(error) = evidence::measure(name, scenario) {
            failures.push(format!("{name}: {error}"));
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "{} qualification scenario(s) failed: {}",
        failures.len(),
        failures.join("; ")
    ))
    .into())
}
