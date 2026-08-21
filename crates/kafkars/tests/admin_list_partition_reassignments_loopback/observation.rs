//! Exact API 46 v0 selection observations for one controller call.

use kafka_wire::ListPartitionReassignmentsRequest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Workflow {
    Selected,
    AllActive,
    BrokerError,
    ControllerRecovery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ListObservation {
    node_id: i32,
    version: i16,
    topics: Option<Vec<(String, Vec<i32>)>>,
}

impl ListObservation {
    pub(super) fn from_request(
        node_id: i32,
        version: i16,
        request: ListPartitionReassignmentsRequest,
    ) -> Self {
        assert!(
            (1..=5_000).contains(&request.timeout_ms),
            "API 46 must derive a positive timeout from the original five-second deadline"
        );
        assert!(
            request.unknown_tagged_fields.is_empty(),
            "API 46 request must not invent top-level tagged fields"
        );
        let topics = request.topics.map(|topics| {
            topics
                .into_iter()
                .map(|topic| {
                    assert!(
                        topic.unknown_tagged_fields.is_empty(),
                        "API 46 request must not invent topic tagged fields"
                    );
                    (topic.name.as_str().to_owned(), topic.partition_indexes)
                })
                .collect()
        });
        Self {
            node_id,
            version,
            topics,
        }
    }

    pub(super) fn expected(workflow: Workflow) -> Self {
        let topics = match workflow {
            Workflow::Selected => Some(vec![
                ("zeta".to_owned(), vec![2, 1]),
                ("missing".to_owned(), vec![9]),
                ("alpha".to_owned(), vec![0]),
            ]),
            Workflow::AllActive | Workflow::BrokerError | Workflow::ControllerRecovery => None,
        };
        Self {
            node_id: 7,
            version: 0,
            topics,
        }
    }
}
