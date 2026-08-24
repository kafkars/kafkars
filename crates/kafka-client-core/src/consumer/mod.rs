//! Deterministic direct, group, and share-consumer ownership policy.
mod assignment_retirement;
mod assignment_retirement_transition;
mod batch_control;
mod classic_group;
mod close;
mod consumer_group;
mod delivery_ownership;
mod effect;
mod error;
mod exports;
mod fetch_state;
mod fetch_throttle;
mod fetch_transition;
mod group_commit;
mod group_position;
mod identity;
mod incremental_assignment;
mod input;
mod machine;
mod model;
mod position;
mod position_failure;
mod position_ownership;
mod position_resolution;
mod position_state;
mod read_isolation;
mod resolved_assignment;
mod resolved_assignment_install;
mod share_consumer;
mod share_fetch;
mod transition;
pub use exports::*;
#[cfg(test)]
mod assignment_retirement_input_test;
#[cfg(test)]
mod assignment_retirement_test;
#[cfg(test)]
mod assignment_retirement_transition_test;
#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod batch_control_test;
#[cfg(test)]
mod close_completion_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod control_test;
#[cfg(test)]
mod delivery_ownership_test;
#[cfg(test)]
mod fetch_delivery_test;
#[cfg(test)]
mod fetch_state_test;
#[cfg(test)]
mod fetch_throttle_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod incremental_assignment_edge_test;
#[cfg(test)]
mod incremental_assignment_reconciliation_test;
#[cfg(test)]
mod incremental_assignment_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod position_failure_test;
#[cfg(test)]
mod position_failure_transition_test;
#[cfg(test)]
mod position_ownership_test;
#[cfg(test)]
mod position_state_test;
#[cfg(test)]
mod position_test;
#[cfg(test)]
mod read_isolation_test;
#[cfg(test)]
mod resolution_test;
#[cfg(test)]
mod resolved_assignment_install_test;
#[cfg(test)]
mod resolved_assignment_rejection_test;
#[cfg(test)]
mod resolved_assignment_test;
#[cfg(test)]
mod throttle_test;
#[cfg(test)]
mod transition_test;
