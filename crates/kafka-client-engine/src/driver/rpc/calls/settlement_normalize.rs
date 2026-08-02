//! O(1) correlation of one tracked Produce entry after aggregate shape validation.

use kafka_client_core::{DeliveryStatus, Moment, ProducerAttemptFailureKind, ProducerInput};
use kafka_driver::RequestError;
use kafka_wire::ProduceResponse;

use crate::protocol::{
    produce_outcome::{
        batched_produce_response_input, explicit_produce_response_input,
        produce_transport_failure_input,
    },
    produce_response_batch::BatchedProduceResponseIndex,
};

use super::super::produce_call_entries::TrackedProduceEntry;

pub(super) fn normalized_entry_input(
    entry: &TrackedProduceEntry,
    now: Moment,
    result: &Result<ProduceResponse, RequestError>,
    batched: bool,
    response_shape_valid: bool,
    response_index: Option<&BatchedProduceResponseIndex>,
) -> ProducerInput {
    if batched && !response_shape_valid {
        return invalid_response(entry, now);
    }
    match result {
        Ok(response) => {
            let normalized = if batched {
                let Some(index) = response_index else {
                    return invalid_response(entry, now);
                };
                batched_produce_response_input(
                    entry.execution,
                    now,
                    entry.topic.as_ref(),
                    entry.partition,
                    response,
                    index,
                )
            } else {
                explicit_produce_response_input(
                    entry.execution,
                    now,
                    entry.topic.as_ref(),
                    entry.partition,
                    response,
                )
            };
            normalized.unwrap_or_else(|failure| {
                produce_transport_failure_input(
                    entry.execution,
                    now,
                    ProducerAttemptFailureKind::InvalidResponse,
                    failure.delivery(),
                )
            })
        }
        Err(error) => produce_transport_failure_input(
            entry.execution,
            now,
            crate::driver::request_failure_kind(error),
            crate::driver::request_failure_delivery(error),
        ),
    }
}

fn invalid_response(entry: &TrackedProduceEntry, now: Moment) -> ProducerInput {
    produce_transport_failure_input(
        entry.execution,
        now,
        ProducerAttemptFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    )
}
