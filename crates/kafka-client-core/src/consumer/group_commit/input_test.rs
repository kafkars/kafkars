//! Closed group offset-commit fact normalization evidence.

use crate::{
    DeliveryStatus, GroupOffsetCommitInput, GroupOffsetCommitPartitionOutcome, PartitionIndex,
    TopicId,
};

#[test]
fn broker_and_transport_inputs_retain_only_normalized_scalar_facts() {
    let response = GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms: 17,
        outcomes: vec![GroupOffsetCommitPartitionOutcome::committed(
            TopicId::from_raw(7),
            PartitionIndex::from_raw(3),
        )],
    };
    let GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms,
        outcomes,
    } = response
    else {
        panic!("broker response fact");
    };
    assert_eq!(throttle_time_ms, 17);
    assert_eq!(outcomes[0].topic_id(), TopicId::from_raw(7));
    assert_eq!(outcomes[0].partition(), PartitionIndex::from_raw(3));

    let transport = GroupOffsetCommitInput::TransportFailed {
        delivery: DeliveryStatus::PossiblySent,
    };
    assert_eq!(
        transport,
        GroupOffsetCommitInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
}
