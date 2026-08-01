//! Exact core-to-engine assignment shape validation during KIP-848 reconciliation.

use kafka_client_core::LiveGroupAssignment;

use super::super::registry_entry::GroupConsumerEntry;

pub(super) fn reconciliation_core_matches(
    entry: &GroupConsumerEntry,
    previous: Option<&LiveGroupAssignment>,
    assignment: &LiveGroupAssignment,
) -> bool {
    let Some(execution) = entry.consumer.as_ref() else {
        return false;
    };
    match previous {
        Some(previous) => {
            execution.machine().live_assignment() == Some(previous)
                && execution.machine().pending_assignment() == Some(assignment)
        }
        None => {
            execution.machine().live_assignment() == Some(assignment)
                && execution.machine().pending_assignment().is_none()
        }
    }
}
