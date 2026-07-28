//! Focused generated request adaptation tests.

use kafka_client_core::LeaderElectionType;

use super::{LeaderElectionRef, elect_leaders_request};

#[test]
fn request_groups_topics_and_preserves_partition_order_within_each_topic() {
    let targets = [
        LeaderElectionRef::new("orders", 2),
        LeaderElectionRef::new("audit", 0),
        LeaderElectionRef::new("orders", 1),
    ];
    let request = elect_leaders_request(LeaderElectionType::Unclean, &targets, 91, usize::MAX)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"));

    assert_eq!(request.election_type, 1);
    assert_eq!(request.timeout_ms, 91);
    let topics = request
        .topic_partitions
        .unwrap_or_else(|| panic!("expected selected partitions"));
    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].topic.as_str(), "audit");
    assert_eq!(topics[0].partitions, vec![0]);
    assert_eq!(topics[1].topic.as_str(), "orders");
    assert_eq!(topics[1].partitions, vec![2, 1]);
}

#[test]
fn request_rejects_invalid_timeout_and_insufficient_scratch() {
    let targets = [LeaderElectionRef::new("orders", 0)];
    assert!(
        elect_leaders_request(LeaderElectionType::Preferred, &targets, -1, usize::MAX).is_err()
    );
    assert!(elect_leaders_request(LeaderElectionType::Preferred, &targets, 10, 0).is_err());
}
