//! Exact API-key 55 request-shape and version evidence.

use kafka_wire::KafkaMessage;
use kafka_wire_core::{ApiVersion, KafkaEncode};

use super::{describe_metadata_quorum_request, request::METADATA_TOPIC};

#[test]
fn request_selects_only_cluster_metadata_partition_zero() {
    let request = describe_metadata_quorum_request();
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].topic_name.as_str(), METADATA_TOPIC);
    assert_eq!(request.topics[0].partitions.len(), 1);
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
}

#[test]
fn exact_request_is_representable_across_v0_v2() {
    let request = describe_metadata_quorum_request();
    for version in 0..=2 {
        let version = ApiVersion::new(version);
        assert!(kafka_wire::DescribeQuorumRequest::SUPPORTED_VERSIONS.contains(version));
        assert!(request.encoded_len(version).is_ok());
    }
    assert!(request.encoded_len(ApiVersion::new(3)).is_err());
}
