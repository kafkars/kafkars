//! Exhaustive stable translation of topic-ID `DeleteTopics` outcomes.

use kafka_client_engine::{DeleteTopicsObserverError, DeleteTopicsOutcome};

use crate::{DeliveryStatus, ErrorKind, KafkaError, admin::BatchResult};

use super::{
    admin_delete_by_id_operation::AdminDeleteTopicsByIdResult,
    admin_delete_result::{translate_failure, translate_observer_error, translate_topic_error},
};

pub(super) fn translate_observation(
    result: Result<DeleteTopicsOutcome, DeleteTopicsObserverError>,
) -> AdminDeleteTopicsByIdResult {
    match result {
        Ok(DeleteTopicsOutcome::TopicIds(topics)) => Ok(BatchResult::new(
            topics
                .into_iter()
                .map(|topic| {
                    let (topic_id, result) = topic.into_parts();
                    (topic_id, result.map_err(translate_topic_error))
                })
                .collect(),
        )),
        Ok(DeleteTopicsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(DeleteTopicsOutcome::Topics(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "topic-ID DeleteTopics received a name-keyed terminal",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}
