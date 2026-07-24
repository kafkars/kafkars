//! Exact-sentinel validation for partition and aborted-transaction facts.

use kafka_wire::{
    FetchResponse,
    fetch_response::{AbortedTransaction, FetchableTopicResponse, PartitionData},
};

use super::{
    FetchDecodeFailure, FetchDecodeLimits, failure::FetchPartitionOffset, normalize_fetch_response,
};

#[test]
fn partition_offset_facts_accept_only_minus_one_as_absent() {
    let normalized = normalize_fetch_response(response(PartitionData::default()), limits())
        .unwrap_or_else(|error| panic!("default partition sentinels: {error:?}"));
    let partition = &normalized.topics[0].partitions[0];
    assert_eq!(partition.last_stable_offset, None);
    assert_eq!(partition.log_start_offset, None);

    for (fact, mutate) in [
        (
            FetchPartitionOffset::HighWatermark,
            set_high_watermark as fn(&mut PartitionData),
        ),
        (
            FetchPartitionOffset::LastStableOffset,
            set_last_stable_offset,
        ),
        (FetchPartitionOffset::LogStartOffset, set_log_start_offset),
    ] {
        let mut partition = PartitionData::default();
        mutate(&mut partition);
        assert_eq!(
            normalize_fetch_response(response(partition), limits()),
            Err(FetchDecodeFailure::InvalidPartitionOffset { fact, actual: -2 })
        );
    }
}

#[test]
fn leader_and_aborted_transaction_negatives_are_not_collapsed() {
    let mut leader = PartitionData::default();
    leader.current_leader.leader_id = -2;
    assert_eq!(
        normalize_fetch_response(response(leader), limits()),
        Err(FetchDecodeFailure::InvalidCurrentLeader {
            leader_id: -2,
            leader_epoch: -1,
        })
    );

    for (producer_id, first_offset) in [(-1, 0), (0, -1)] {
        let mut partition = PartitionData::default();
        let mut transaction = AbortedTransaction::default();
        transaction.producer_id = producer_id;
        transaction.first_offset = first_offset;
        partition.aborted_transactions = Some(vec![transaction]);
        assert_eq!(
            normalize_fetch_response(response(partition), limits()),
            Err(FetchDecodeFailure::InvalidAbortedTransaction {
                producer_id,
                first_offset,
            })
        );
    }
}

fn response(partition: PartitionData) -> FetchResponse {
    let mut topic = FetchableTopicResponse::default();
    topic.topic = "events".into();
    topic.partitions = vec![partition];
    let mut response = FetchResponse::default();
    response.responses = vec![topic];
    response
}

fn limits() -> FetchDecodeLimits {
    FetchDecodeLimits::default()
}

fn set_high_watermark(partition: &mut PartitionData) {
    partition.high_watermark = -2;
}

fn set_last_stable_offset(partition: &mut PartitionData) {
    partition.last_stable_offset = -2;
}

fn set_log_start_offset(partition: &mut PartitionData) {
    partition.log_start_offset = -2;
}
