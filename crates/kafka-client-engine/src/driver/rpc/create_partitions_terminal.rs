//! Semantic terminal normalization for tracked `CreatePartitions` calls.

use kafka_client_core::CreatePartitionsInput;
use kafka_driver::RequestError;
use kafka_wire::CreatePartitionsResponse;

use crate::protocol::admin::create_partitions::{
    CreatePartitionsProtocolFailure, normalize_create_partitions_response_bounded,
};

pub(super) fn normalize_terminal(
    plan: &kafka_client_core::CreatePartitionsPlan,
    retained_bytes: usize,
    result: Result<CreatePartitionsResponse, RequestError>,
) -> Result<CreatePartitionsInput, CreatePartitionsProtocolFailure> {
    match result {
        Ok(response) => {
            match normalize_create_partitions_response_bounded(plan, &response, retained_bytes) {
                Ok(outcomes) => Ok(CreatePartitionsInput::BrokerResponded { outcomes }),
                Err(CreatePartitionsProtocolFailure::RetainedBytes) => {
                    Err(CreatePartitionsProtocolFailure::RetainedBytes)
                }
                Err(
                    CreatePartitionsProtocolFailure::TopicCount { .. }
                    | CreatePartitionsProtocolFailure::UnexpectedTopic
                    | CreatePartitionsProtocolFailure::MissingTopic
                    | CreatePartitionsProtocolFailure::DuplicateTopic,
                ) => Ok(CreatePartitionsInput::InvalidResponse),
            }
        }
        Err(error) => Ok(CreatePartitionsInput::TransportFailed {
            delivery: super::super::request_failure_delivery(&error),
        }),
    }
}
