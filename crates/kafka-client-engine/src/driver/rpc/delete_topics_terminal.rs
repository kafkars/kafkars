//! Semantic terminal normalization for tracked `DeleteTopics` calls.

use kafka_client_core::DeleteTopicsInput;
use kafka_driver::RequestError;
use kafka_wire::DeleteTopicsResponse;

use crate::protocol::admin::delete_topics::{
    DeleteTopicsProtocolFailure, normalize_delete_topics_response_bounded,
};

pub(super) fn normalize_terminal(
    plan: &kafka_client_core::DeleteTopicsPlan,
    retained_bytes: usize,
    result: Result<DeleteTopicsResponse, RequestError>,
) -> Result<DeleteTopicsInput, DeleteTopicsProtocolFailure> {
    match result {
        Ok(response) => {
            match normalize_delete_topics_response_bounded(plan, &response, retained_bytes) {
                Ok(outcomes) => Ok(DeleteTopicsInput::BrokerResponded { outcomes }),
                Err(DeleteTopicsProtocolFailure::RetainedBytes) => {
                    Err(DeleteTopicsProtocolFailure::RetainedBytes)
                }
                Err(
                    DeleteTopicsProtocolFailure::TopicCount { .. }
                    | DeleteTopicsProtocolFailure::MissingResponseName
                    | DeleteTopicsProtocolFailure::UnexpectedTopic
                    | DeleteTopicsProtocolFailure::MissingTopic
                    | DeleteTopicsProtocolFailure::DuplicateTopic,
                ) => Ok(DeleteTopicsInput::InvalidResponse),
            }
        }
        Err(error) => Ok(DeleteTopicsInput::TransportFailed {
            delivery: super::super::request_failure_delivery(&error),
        }),
    }
}
