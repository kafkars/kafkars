//! Partition payload, acquisition-range, and retained-budget evidence.

use kafka_wire::share_fetch_response::{AcquiredRecords, PartitionData};
use kafka_wire_core::Bytes;

use super::{
    ShareFetchCorrelation, ShareFetchRequestTopic, ShareFetchResponseFailure,
    ShareFetchResponseLimits,
    response_partition::{ShareFetchBudget, normalize_partition},
};

#[test]
fn partition_rejects_overlap_and_preserves_exact_resource_bounds() {
    let correlation = ShareFetchCorrelation::new(vec![topic(1, &[0])]);

    let mut overlap = partition(0, 0, 2, 1);
    overlap.acquired_records.push(acquired(2, 4, 1));
    assert_eq!(
        normalize_partition(
            overlap,
            id(1),
            &correlation,
            &mut ShareFetchBudget::new(ShareFetchResponseLimits::new(8, 16)),
        ),
        Err(ShareFetchResponseFailure::OverlappingAcquiredRange)
    );

    assert_eq!(
        normalize_partition(
            partition(0, 0, 2, 1),
            id(1),
            &correlation,
            &mut ShareFetchBudget::new(ShareFetchResponseLimits::new(2, 16)),
        ),
        Err(ShareFetchResponseFailure::RecordCount {
            actual: 3,
            limit: 2,
        })
    );

    assert_eq!(
        normalize_partition(
            partition(0, 0, 0, 1),
            id(1),
            &correlation,
            &mut ShareFetchBudget::new(ShareFetchResponseLimits::new(8, 2)),
        ),
        Err(ShareFetchResponseFailure::RetainedBytes {
            actual: 3,
            limit: 2,
        })
    );
}

fn partition(index: i32, first: i64, last: i64, delivery_count: i16) -> PartitionData {
    let mut partition = PartitionData::default();
    partition.partition_index = index;
    partition.records = Bytes::from_static(b"raw");
    partition.acquired_records = vec![acquired(first, last, delivery_count)];
    partition
}

fn acquired(first: i64, last: i64, delivery_count: i16) -> AcquiredRecords {
    let mut acquired = AcquiredRecords::default();
    acquired.first_offset = first;
    acquired.last_offset = last;
    acquired.delivery_count = delivery_count;
    acquired
}

fn topic(value: u8, partitions: &[u32]) -> ShareFetchRequestTopic {
    ShareFetchRequestTopic::try_new(id(value), partitions.to_vec())
        .unwrap_or_else(|error| panic!("valid topic: {error:?}"))
}

fn id(value: u8) -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = value;
    id
}
