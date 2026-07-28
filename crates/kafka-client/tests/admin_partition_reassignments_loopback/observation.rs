//! Canonical API 45 v1 request observations including null cancellation targets.

use kafka_wire::AlterPartitionReassignmentsRequest;

use super::frame::RequestFrame;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Workflow {
    Standard,
    ControllerRecovery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PartitionObservation {
    partition: i32,
    replicas: Option<Vec<i32>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TopicObservation {
    name: String,
    partitions: Vec<PartitionObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequestObservation {
    node_id: i32,
    version: i16,
    timeout_ms: i32,
    allow_replication_factor_change: bool,
    topics: Vec<TopicObservation>,
}

impl RequestObservation {
    pub(super) fn decode(request: &RequestFrame, node_id: i32) -> Self {
        let decoded: AlterPartitionReassignmentsRequest = request.decode();
        assert!(
            decoded.unknown_tagged_fields.is_empty(),
            "API 45 request must not invent top-level tagged fields"
        );
        let topics = decoded
            .topics
            .into_iter()
            .map(|topic| {
                assert!(
                    topic.unknown_tagged_fields.is_empty(),
                    "API 45 request must not invent topic tagged fields"
                );
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        assert!(
                            partition.unknown_tagged_fields.is_empty(),
                            "API 45 request must not invent partition tagged fields"
                        );
                        PartitionObservation {
                            partition: partition.partition_index,
                            replicas: partition.replicas,
                        }
                    })
                    .collect();
                TopicObservation {
                    name: topic.name.as_str().to_owned(),
                    partitions,
                }
            })
            .collect();
        Self {
            node_id,
            version: request.api_version.value(),
            timeout_ms: decoded.timeout_ms,
            allow_replication_factor_change: decoded.allow_replication_factor_change,
            topics,
        }
    }

    pub(super) fn assert_exact(&self) {
        assert_eq!(self.node_id, 7);
        assert_eq!(self.version, 1, "API 45 must use the client v1 ceiling");
        assert!((1..=5_000).contains(&self.timeout_ms));
        assert!(!self.allow_replication_factor_change);
        assert_eq!(
            self.topics,
            [
                topic("alpha", vec![partition(3, None)]),
                topic("beta", vec![partition(4, Some(vec![9, 2, 5]))]),
                topic(
                    "zeta",
                    vec![partition(2, Some(vec![7, 3])), partition(0, None),],
                ),
            ],
            "API 45 must group topics canonically while preserving within-topic caller order and null cancellation"
        );
    }
}

fn topic(name: &str, partitions: Vec<PartitionObservation>) -> TopicObservation {
    TopicObservation {
        name: name.to_owned(),
        partitions,
    }
}

fn partition(partition: i32, replicas: Option<Vec<i32>>) -> PartitionObservation {
    PartitionObservation {
        partition,
        replicas,
    }
}
