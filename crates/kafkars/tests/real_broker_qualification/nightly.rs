//! Independent nightly scenario sequencing with complete failure evidence.

use std::io;

use crate::real_broker_support::TestError;

use super::{
    evidence, nightly_admin, nightly_consumer, nightly_group, nightly_producer, nightly_resilience,
    nightly_resources, nightly_transaction,
};

type Scenario = (&'static str, fn() -> Result<(), TestError>);

const SCENARIOS: [Scenario; 13] = [
    (
        "producer_batching_partitioning",
        nightly_producer::batching_and_partitioning,
    ),
    (
        "producer_retries_leader_movement",
        nightly_resilience::producer_retries_leader_movement,
    ),
    (
        "producer_cancellation_ambiguous_delivery",
        nightly_producer::cancellation_and_ambiguity,
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
        "classic_member_death_commit_resume",
        nightly_group::member_death_commit_and_resume,
    ),
    (
        "kip848_assignment_reconciliation",
        nightly_group::kip848_assignment_and_reconciliation,
    ),
    (
        "transaction_fencing_abort_commit_read_committed",
        nightly_transaction::fencing_abort_commit_and_read_committed,
    ),
    (
        "admin_controller_coordinator_exact_broker",
        nightly_admin::controller_coordinator_and_exact_broker,
    ),
    (
        "broker_restart_metadata_refresh",
        nightly_resilience::broker_restart_metadata_refresh,
    ),
    (
        "coordinator_loss_leader_change",
        nightly_resilience::coordinator_loss_and_leader_change,
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
    let mut failures = Vec::new();
    for (name, scenario) in SCENARIOS {
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
