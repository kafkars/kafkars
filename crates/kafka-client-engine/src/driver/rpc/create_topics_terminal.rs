//! Semantic terminal normalization for tracked `CreateTopics` calls.

use kafka_client_core::CreateTopicsInput;
use kafka_driver::RequestError;
use kafka_wire::CreateTopicsResponse;

use crate::protocol::admin::create_topics::{
    CreateTopicsProtocolFailure, normalize_create_topics_response_bounded,
};

pub(super) fn normalize_terminal(
    plan: &kafka_client_core::CreateTopicsPlan,
    retained_bytes: usize,
    result: Result<CreateTopicsResponse, RequestError>,
) -> Result<CreateTopicsInput, CreateTopicsProtocolFailure> {
    match result {
        Ok(response) => {
            match normalize_create_topics_response_bounded(plan, &response, retained_bytes) {
                Ok(outcomes) => Ok(CreateTopicsInput::BrokerResponded { outcomes }),
                Err(CreateTopicsProtocolFailure::RetainedBytes) => {
                    Err(CreateTopicsProtocolFailure::RetainedBytes)
                }
                Err(
                    CreateTopicsProtocolFailure::TopicCount { .. }
                    | CreateTopicsProtocolFailure::UnexpectedTopic { .. }
                    | CreateTopicsProtocolFailure::MissingTopic { .. }
                    | CreateTopicsProtocolFailure::DuplicateTopic { .. },
                ) => Ok(CreateTopicsInput::InvalidResponse),
            }
        }
        Err(error) => Ok(CreateTopicsInput::TransportFailed {
            delivery: super::super::request_failure_delivery(&error),
        }),
    }
}
