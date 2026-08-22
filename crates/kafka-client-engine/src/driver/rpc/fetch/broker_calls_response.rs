//! Pre-reserved response slots and allocation-free broker Fetch fan-out.

use kafka_client_core::Moment;
use kafka_wire::{FetchResponse as WireFetchResponse, fetch_response::FetchableTopicResponse};
use kafka_wire_core::Uuid;

#[cfg(test)]
use super::broker_calls::SettledBrokerFetchBatch;
use super::{
    admission::{FetchAdmissionFailureSource, PartitionFetchRequest},
    broker_calls::{BrokerFetchSlot, TrackedBrokerFetchCalls},
    terminal::retain_fetch_terminal,
};

pub(super) fn reserved_responses(
    requests: &[PartitionFetchRequest],
) -> Result<Vec<WireFetchResponse>, FetchAdmissionFailureSource> {
    let mut responses = Vec::new();
    responses
        .try_reserve_exact(requests.len())
        .map_err(|_error| allocation())?;
    for request in requests {
        let mut response = WireFetchResponse::default();
        response
            .responses
            .try_reserve_exact(1)
            .map_err(|_error| allocation())?;
        let mut topic = FetchableTopicResponse::default();
        topic.topic = request.topic().into();
        if let Some(route) = request.topic_route() {
            topic.topic_id = Uuid::from_bytes(route.topic_id());
        }
        topic
            .partitions
            .try_reserve_exact(1)
            .map_err(|_error| allocation())?;
        response.responses.push(topic);
        responses.push(response);
    }
    Ok(responses)
}

pub(super) fn distribute_terminal(
    slots: &mut [BrokerFetchSlot],
    now: Moment,
    selected_version: Option<kafka_driver::ApiVersion>,
    result: Result<WireFetchResponse, kafka_driver::RequestError>,
) {
    match result {
        Err(error) => {
            for slot in slots {
                if let Some(request) = slot.request.take() {
                    slot.terminal = Some(retain_fetch_terminal(
                        request,
                        now,
                        selected_version,
                        Err(error.clone()),
                    ));
                }
            }
        }
        Ok(response) => distribute_response(slots, now, selected_version, response),
    }
}

fn distribute_response(
    slots: &mut [BrokerFetchSlot],
    now: Moment,
    selected_version: Option<kafka_driver::ApiVersion>,
    mut response: WireFetchResponse,
) {
    for slot in slots.iter_mut() {
        slot.response.throttle_time_ms = response.throttle_time_ms;
        slot.response.error_code = response.error_code;
        slot.response.session_id = response.session_id;
    }
    let mut invalid = false;
    let topic_ids = selected_version.is_some_and(|version| version.value() >= 13);
    for mut topic in response.responses.drain(..) {
        if topic.partitions.is_empty() {
            invalid = true;
        }
        for partition in topic.partitions.drain(..) {
            let mut matching = slots.iter().enumerate().filter_map(|(index, slot)| {
                slot.request
                    .as_ref()
                    .is_some_and(|request| {
                        ((topic_ids
                            && request.topic_route().is_some_and(|route| {
                                route.topic_id() == topic.topic_id.to_bytes()
                            }))
                            || (!topic_ids && request.topic() == topic.topic.as_str()))
                            && request.fence().position().partition().partition().get()
                                == u32::try_from(partition.partition_index).unwrap_or(u32::MAX)
                    })
                    .then_some(index)
            });
            let Some(index) = matching.next() else {
                invalid = true;
                continue;
            };
            if matching.next().is_some() {
                invalid = true;
                continue;
            }
            let target = &mut slots[index].response.responses[0];
            if target.partitions.is_empty() {
                target.topic_id = topic.topic_id;
                target.partitions.push(partition);
            } else {
                invalid = true;
            }
        }
    }
    for slot in slots {
        if invalid {
            mark_invalid(&mut slot.response);
        } else if slot.response.responses[0].partitions.is_empty() {
            slot.response.responses.clear();
        }
        if let Some(request) = slot.request.take() {
            let response = std::mem::take(&mut slot.response);
            slot.terminal = Some(retain_fetch_terminal(
                request,
                now,
                selected_version,
                Ok(response),
            ));
        }
    }
}

fn mark_invalid(response: &mut WireFetchResponse) {
    let topic = &mut response.responses[0];
    if let Some(partition) = topic.partitions.first_mut() {
        partition.partition_index = i32::MIN;
    } else {
        let mut partition = kafka_wire::fetch_response::PartitionData::default();
        partition.partition_index = i32::MIN;
        topic.partitions.push(partition);
    }
}

const fn allocation() -> FetchAdmissionFailureSource {
    FetchAdmissionFailureSource::Request(crate::protocol::fetch::FetchRequestFailure::Allocation)
}

impl TrackedBrokerFetchCalls {
    #[cfg(test)]
    pub(crate) fn install_response_for_test(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        now: Moment,
        selected_version: i16,
        response: WireFetchResponse,
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
        distribute_response(
            &mut slots,
            now,
            Some(kafka_driver::ApiVersion::new(selected_version)),
            response,
        );
        self.settled = Some(SettledBrokerFetchBatch {
            slots,
            route_token: None,
        });
    }

    #[cfg(test)]
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
                let mut partition = kafka_wire::fetch_response::PartitionData::default();
                partition.partition_index =
                    i32::try_from(request.fence().position().partition().partition().get())
                        .unwrap_or_else(|error| panic!("test partition must fit i32: {error}"));
                partition.error_code = *error_code;
                partition
            })
            .collect();
        let mut response = WireFetchResponse::default();
        response.session_id = session_id;
        response.responses = vec![topic];
        self.install_response_for_test(requests, now, selected_version, response);
    }

    #[cfg(test)]
    pub(crate) fn install_broker_error_for_test(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        now: Moment,
        selected_version: i16,
        error_code: i16,
    ) {
        let mut response = WireFetchResponse::default();
        response.error_code = error_code;
        self.install_response_for_test(requests, now, selected_version, response);
    }
}
