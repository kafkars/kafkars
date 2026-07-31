//! Dormant KIP-848 explicit-close completion evidence.

use std::sync::Arc;

use kafka_client_core::{GroupId, GroupPositionMissingOffsetPolicy, Moment, ReadIsolation};

use super::{
    classic_group_leave::{
        GroupConsumerCloseCompletion, GroupConsumerCloseCompletionObservation,
        GroupConsumerCloseTerminal,
    },
    registry::GroupConsumerRegistry,
    registry_entry::{
        GroupConsumerEntry, GroupConsumerEntryState, default_classic_processing_lease_policy,
    },
    registry_test_support::deadline,
};
use crate::consumer::group_registration_request::GroupConsumerProtocol;

#[test]
fn dormant_modern_close_completes_without_submitting_a_leave() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut entry = GroupConsumerEntry::try_new_with_protocol_configuration(
        group_id,
        &Arc::from("workers"),
        None,
        &[Arc::from("orders")],
        GroupConsumerProtocol::Consumer,
        super::classic_group_test_support::timing(),
        super::classic_group_test_support::heartbeat_policy(),
        super::classic_group_test_support::rejoin_policy(),
        GroupPositionMissingOffsetPolicy::Error,
        ReadIsolation::ReadUncommitted,
        default_classic_processing_lease_policy(),
    )
    .unwrap_or_else(|error| panic!("entry: {error:?}"));
    let completion = Arc::new(GroupConsumerCloseCompletion::pending());
    entry
        .leave
        .begin(deadline(20), Arc::clone(&completion))
        .unwrap_or_else(|_completion| panic!("close admission"));
    entry.state = GroupConsumerEntryState::Closing;
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);

    assert_eq!(
        registry.turn_one_consumer_group_close(Moment::from_tick(10)),
        Ok(super::consumer_group_close::ConsumerGroupCloseTurn::Progress)
    );
    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("entry"));
    assert!(entry.leave.publish_terminal());
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Succeeded)
    );
}
