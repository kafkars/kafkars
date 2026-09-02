//! Generated v16 broker response fixtures for aggregate Fetch owner tests.

use kafka_client_core::Moment;
use kafka_wire::{
    FetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};
use kafka_wire_core::Uuid;

use super::{
    admission::PartitionFetchRequest,
    broker_calls::{
        BrokerFetchCompletionFailure, BrokerFetchSlot, SettledBrokerFetchBatch,
        TrackedBrokerFetchCalls,
    },
    broker_calls_response::{distribute_terminal, reserved_responses},
    terminal::FetchCompletionObservation,
};

impl TrackedBrokerFetchCalls {
    pub(crate) fn install_response_for_test(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        now: Moment,
        selected_version: i16,
        response: FetchResponse,
    ) {
        let responses = reserved_responses(&requests)
            .unwrap_or_else(|error| panic!("reserve broker response slots: {error:?}"));
        let mut slots = requests
            .into_iter()
            .zip(responses)
            .map(|(request, response)| BrokerFetchSlot {
                fence: request.fence(),
                request: Some(request),
                response,
                terminal: None,
            })
            .collect::<Vec<_>>();
        distribute_terminal(
            &mut slots,
            now,
            Some(kafka_driver::ApiVersion::new(selected_version)),
            Ok(response),
        );
        self.settled = Some(SettledBrokerFetchBatch {
            slots,
            route_token: None,
        });
    }

    pub(crate) fn install_response_after_stale_request_for_test(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        stale: kafka_client_core::FetchFence,
        now: Moment,
        selected_version: i16,
        response: FetchResponse,
    ) {
        let responses = reserved_responses(&requests)
            .unwrap_or_else(|error| panic!("reserve broker response slots: {error:?}"));
        let mut slots = requests
            .into_iter()
            .zip(responses)
            .map(|(request, response)| BrokerFetchSlot {
                fence: request.fence(),
                request: Some(request),
                response,
                terminal: None,
            })
            .collect::<Vec<_>>();
        let slot = slots
            .iter_mut()
            .find(|slot| slot.fence == stale)
            .unwrap_or_else(|| panic!("stale Fetch slot"));
        drop(slot.request.take());
        distribute_terminal(
            &mut slots,
            now,
            Some(kafka_driver::ApiVersion::new(selected_version)),
            Ok(response),
        );
        self.settled = Some(SettledBrokerFetchBatch {
            slots,
            route_token: None,
        });
    }

    pub(crate) fn install_topic_partition_results_for_test(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        now: Moment,
        selected_version: i16,
        session_id: i32,
        error_codes: &[i16],
    ) {
        assert_eq!(requests.len(), error_codes.len());
        let topic_name = requests.first().map_or_else(
            || panic!("test response requires one request"),
            |request| request.topic().to_owned(),
        );
        let mut topic = FetchableTopicResponse::default();
        topic.topic = topic_name.into();
        if let Some(route) = requests
            .first()
            .and_then(PartitionFetchRequest::topic_route)
        {
            topic.topic_id = Uuid::from_bytes(route.topic_id());
        }
        topic.partitions = requests
            .iter()
            .zip(error_codes)
            .map(|(request, error_code)| {
                let mut partition = PartitionData::default();
                partition.partition_index =
                    i32::try_from(request.fence().position().partition().partition().get())
                        .unwrap_or_else(|error| panic!("test partition must fit i32: {error}"));
                partition.error_code = *error_code;
                partition
            })
            .collect();
        let mut response = FetchResponse::default();
        response.session_id = session_id;
        response.responses = vec![topic];
        self.install_response_for_test(requests, now, selected_version, response);
    }

    pub(crate) fn install_broker_error_for_test(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        now: Moment,
        selected_version: i16,
        error_code: i16,
    ) {
        let mut response = FetchResponse::default();
        response.error_code = error_code;
        self.install_response_for_test(requests, now, selected_version, response);
    }

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
