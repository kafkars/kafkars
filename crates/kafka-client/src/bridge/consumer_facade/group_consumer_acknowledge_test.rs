//! Private synchronous checkpoint-acknowledgment translation contract.

use kafka_client_engine::GroupConsumerAcknowledgeErrorKind;

use super::{
    group_consumer::GroupConsumerEngine,
    group_consumer_acknowledge::translate_group_consumer_acknowledgment,
    group_consumer_checkpoint::GroupConsumerCheckpoint,
};
use crate::{ErrorKind, KafkaError};

type Acknowledge = fn(
    &mut GroupConsumerEngine,
    GroupConsumerCheckpoint,
) -> Result<(), (GroupConsumerCheckpoint, KafkaError)>;

#[test]
fn rejection_returns_the_exact_private_checkpoint_owner() {
    let _: Acknowledge = GroupConsumerEngine::acknowledge;
}

#[test]
fn acknowledgment_categories_translate_exhaustively_without_protocol_work() {
    for kind in [
        GroupConsumerAcknowledgeErrorKind::Closed,
        GroupConsumerAcknowledgeErrorKind::GroupUnavailable,
        GroupConsumerAcknowledgeErrorKind::StaleCheckpoint,
        GroupConsumerAcknowledgeErrorKind::DeadlineElapsed,
    ] {
        assert_eq!(
            translate_group_consumer_acknowledgment(kind).kind(),
            ErrorKind::State
        );
    }
    assert_eq!(
        translate_group_consumer_acknowledgment(GroupConsumerAcknowledgeErrorKind::Contended)
            .kind(),
        ErrorKind::Backpressure
    );
    for kind in [
        GroupConsumerAcknowledgeErrorKind::Clock,
        GroupConsumerAcknowledgeErrorKind::HostUnavailable,
        GroupConsumerAcknowledgeErrorKind::InternalInvariant,
    ] {
        assert_eq!(
            translate_group_consumer_acknowledgment(kind).kind(),
            ErrorKind::Internal
        );
    }
}
