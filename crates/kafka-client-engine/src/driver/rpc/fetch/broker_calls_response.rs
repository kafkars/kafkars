//! Pre-reserved response slots and allocation-free broker Fetch fan-out.

use kafka_client_core::Moment;
use kafka_wire::{FetchResponse as WireFetchResponse, fetch_response::FetchableTopicResponse};
use kafka_wire_core::Uuid;

use super::{
    admission::{FetchAdmissionFailureSource, PartitionFetchRequest},
    broker_calls::BrokerFetchSlot,
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
    let incremental = slots
        .iter()
        .find_map(|slot| slot.request.as_ref())
        .is_some_and(|request| request.session().is_incremental());
    let topic_ids = selected_version.is_some_and(|version| version.value() >= 13);
    for mut topic in response.responses.drain(..) {
        if topic.partitions.is_empty() {
            invalid = true;
        }
        for partition in topic.partitions.drain(..) {
            let mut matching = slots.iter().enumerate().filter_map(|(index, slot)| {
                let reserved = slot.response.responses.first()?;
                let topic_matches = if topic_ids {
                    reserved.topic_id == topic.topic_id
                } else {
                    reserved.topic == topic.topic
                };
                (topic_matches
                    && slot.fence.position().partition().partition().get()
                        == u32::try_from(partition.partition_index).unwrap_or(u32::MAX))
                .then_some(index)
            });
            let Some(index) = matching.next() else {
                // An established broker Fetch session can return data for a
                // cached member that was not part of this request delta. Its
                // unchanged fetch offset keeps that data replayable, so the
                // current exact-partition slots may safely ignore it.
                if !incremental {
                    invalid = true;
                }
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
