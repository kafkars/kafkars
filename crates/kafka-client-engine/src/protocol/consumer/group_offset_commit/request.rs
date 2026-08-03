//! Generated v2-v9 request construction from one prepared commit snapshot.

use kafka_wire::{
    OffsetCommitRequest,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use kafka_wire_core::StrBytes;

use super::PreparedGroupOffsetCommit;

/// Builds API-key 8 input without transport, retry, or coordinator policy.
pub(crate) fn group_offset_commit_request(
    prepared: &PreparedGroupOffsetCommit,
) -> OffsetCommitRequest {
    let mut request = OffsetCommitRequest::default();
    request.group_id = prepared.group().as_ref().into();
    request.generation_id_or_member_epoch = prepared.generation_id_or_member_epoch();
    request.member_id = prepared.member().as_ref().into();
    request.retention_time_ms = -1;
    for entry in prepared.entries() {
        let mut partition = OffsetCommitRequestPartition::default();
        partition.partition_index = entry.partition_index();
        partition.committed_offset = entry.next_offset();
        partition.committed_leader_epoch = entry.leader_epoch().unwrap_or(-1);
        partition.committed_metadata = Some(StrBytes::default());
        if let Some(topic) = request
            .topics
            .last_mut()
            .filter(|topic| topic.name.as_str() == entry.topic().as_ref())
        {
            topic.partitions.push(partition);
        } else {
            let mut topic = OffsetCommitRequestTopic::default();
            topic.name = entry.topic().as_ref().into();
            topic.partitions.push(partition);
            request.topics.push(topic);
        }
    }
    request
}
