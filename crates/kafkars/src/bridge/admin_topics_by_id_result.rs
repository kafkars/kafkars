//! Exhaustive stable translation of topic-ID-keyed engine descriptions.

use kafka_client_engine::{DescribeTopicsObserverError, DescribeTopicsOutcome};

use crate::{DeliveryStatus, ErrorKind, KafkaError, admin::BatchResult};

use super::{
    admin_topics_by_id_operation::AdminDescribeTopicsByIdResult,
    admin_topics_result::{
        translate_description, translate_failure, translate_observer_error, translate_topic_error,
    },
};

pub(super) fn translate_observation(
    result: Result<DescribeTopicsOutcome, DescribeTopicsObserverError>,
) -> AdminDescribeTopicsByIdResult {
    match result {
        Ok(DescribeTopicsOutcome::TopicIds(topics)) => Ok(BatchResult::new(
            topics
                .into_iter()
                .map(|topic| {
                    let (topic_id, result) = topic.into_parts();
                    (
                        topic_id,
                        result
                            .map(translate_description)
                            .map_err(|error| translate_topic_error(error, false)),
                    )
                })
                .collect(),
        )),
        Ok(DescribeTopicsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(DescribeTopicsOutcome::Topics(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "topic-ID DescribeTopics received a name-keyed terminal",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}
