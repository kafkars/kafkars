//! Generated v16 broker response fixtures for aggregate Fetch owner tests.

use kafka_client_core::Moment;
use kafka_wire::{
    FetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};
use kafka_wire_core::Uuid;

use super::{
    admission::PartitionFetchRequest,
    broker_calls::{BrokerFetchCompletionFailure, BrokerFetchSlot, TrackedBrokerFetchCalls},
    terminal::FetchCompletionObservation,
};

impl TrackedBrokerFetchCalls {
    pub(crate) fn install_closed_completion_for_test(&mut self, request: PartitionFetchRequest) {
        let fence = request.fence();
        self.completion_failure = Some(BrokerFetchCompletionFailure {
            slots: vec![BrokerFetchSlot {
                fence,
                request: Some(request),
                response: FetchResponse::default(),
                terminal: None,
            }],
            observation: FetchCompletionObservation::from_driver(
                fence,
                kafka_driver::CompletionError::Closed,
            ),
            source: kafka_driver::CompletionError::Closed,
        });
    }

    pub(crate) fn install_leader_movement_for_test(
        &mut self,
        request: PartitionFetchRequest,
        now: Moment,
        error_code: i16,
        leader: Option<(i32, i32)>,
    ) {
        let mut partition = PartitionData::default();
        partition.partition_index =
            i32::try_from(request.fence().position().partition().partition().get())
                .unwrap_or_else(|error| panic!("partition: {error}"));
        partition.error_code = error_code;
        if let Some((broker_id, epoch)) = leader {
            partition.current_leader.leader_id = broker_id;
            partition.current_leader.leader_epoch = epoch;
        }
        let mut topic = FetchableTopicResponse::default();
        topic.topic_id = Uuid::from_bytes([7; 16]);
        topic.partitions = vec![partition];
        let mut response = FetchResponse::default();
        response.responses = vec![topic];
        self.install_response_for_test(vec![request], now, 16, response);
    }
}
