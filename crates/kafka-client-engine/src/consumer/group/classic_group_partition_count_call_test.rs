//! Exact partition-count call identity scenarios.

use std::time::Instant;

use kafka_client_core::{Deadline, GroupId, MembershipCycle, TopicId};

use crate::clock::OperationDeadline;

use super::classic_group_partition_count_call::ClassicGroupPartitionCountCallIdentity;

#[test]
fn call_identity_retains_group_cycle_topic_and_original_deadline() {
    let group_id = GroupId::try_from_raw(7).unwrap_or_else(|| panic!("group identity"));
    let cycle = MembershipCycle::initial();
    let topic_id = TopicId::from_raw(11);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(19), Instant::now());
    let identity = ClassicGroupPartitionCountCallIdentity::new(group_id, cycle, topic_id, deadline);

    assert_eq!(identity.group_id(), group_id);
    assert_eq!(identity.cycle(), cycle);
    assert_eq!(identity.topic_id(), topic_id);
    assert_eq!(identity.deadline(), deadline);
}
