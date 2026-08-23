//! Public share acknowledgement response-view contract.

use super::{ShareAcknowledgementPartitionOutcome, ShareAcknowledgementResponse};

#[test]
fn response_exposes_correlated_generated_free_partition_facts() {
    fn response_contract(response: &ShareAcknowledgementResponse) {
        let _: u32 = response.throttle_time_ms();
        let _: Vec<ShareAcknowledgementPartitionOutcome<'_>> = response.partitions().collect();
    }
    fn partition_contract(partition: ShareAcknowledgementPartitionOutcome<'_>) {
        let _: [u8; 16] = partition.topic_id();
        let _: u32 = partition.partition();
        let _: Option<i16> = partition.broker_code();
        let _: Option<&[u8]> = partition.error_message();
        let _: Option<(i32, i32)> = partition.current_leader();
    }

    let _ = response_contract as fn(&ShareAcknowledgementResponse);
    let _ = partition_contract as fn(ShareAcknowledgementPartitionOutcome<'_>);
}
