//! Terminal response, broker rejection, and delivery-certainty evidence.

use kafka_client_core::{DeliveryStatus, ShareFetchBrokerId};
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::{
    ShareAcknowledgeResponse,
    share_acknowledge_response::{PartitionData, ShareAcknowledgeTopicResponse},
};
use kafka_wire_core::Uuid;

use crate::protocol::consumer::share_acknowledge::test_support::{id, prepared_request};

use super::{
    ShareAcknowledgeDriverFailureKind, ShareAcknowledgeResolution,
    call::ShareAcknowledgeCallEvidence, terminal::retain_share_acknowledge_terminal,
};

#[test]
fn terminal_normalizes_complete_success_and_exact_broker_route() {
    let broker = broker();
    let terminal = retain_share_acknowledge_terminal(
        evidence(broker),
        Some(ApiVersion::new(1)),
        Ok(complete_response()),
        None,
    );
    let (resolution, route) = terminal.into_resolution();
    let ShareAcknowledgeResolution::Succeeded(success) = resolution else {
        panic!("success expected");
    };
    assert_eq!(success.outcomes.len(), 3);
    assert_eq!(route.broker_id(), broker);
    route.accept();
}

#[test]
fn terminal_preserves_broker_code_and_request_delivery_certainty() {
    let mut rejected = ShareAcknowledgeResponse::default();
    rejected.error_code = 16;
    let terminal = retain_share_acknowledge_terminal(
        evidence(broker()),
        Some(ApiVersion::new(1)),
        Ok(rejected),
        None,
    );
    let (resolution, route) = terminal.into_resolution();
    let ShareAcknowledgeResolution::BrokerRejected(rejection) = resolution else {
        panic!("broker rejection expected");
    };
    assert_eq!(rejection.error_code.get(), 16);
    route.accept();

    let terminal = retain_share_acknowledge_terminal(
        evidence(broker()),
        Some(ApiVersion::new(1)),
        Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }),
        None,
    );
    let (resolution, route) = terminal.into_resolution();
    assert_eq!(
        resolution,
        ShareAcknowledgeResolution::Failed {
            kind: ShareAcknowledgeDriverFailureKind::DeadlineElapsed,
            delivery: DeliveryStatus::NotSent,
        }
    );
    route.accept();
}

#[test]
fn missing_version_and_incomplete_success_are_possibly_sent_failures() {
    let terminal =
        retain_share_acknowledge_terminal(evidence(broker()), None, Ok(complete_response()), None);
    let (resolution, route) = terminal.into_resolution();
    assert_eq!(
        resolution,
        ShareAcknowledgeResolution::Failed {
            kind: ShareAcknowledgeDriverFailureKind::Compatibility,
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    route.accept();

    let terminal = retain_share_acknowledge_terminal(
        evidence(broker()),
        Some(ApiVersion::new(1)),
        Ok(ShareAcknowledgeResponse::default()),
        None,
    );
    let (resolution, route) = terminal.into_resolution();
    assert_eq!(
        resolution,
        ShareAcknowledgeResolution::Failed {
            kind: ShareAcknowledgeDriverFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    route.accept();
}

fn evidence(broker_id: ShareFetchBrokerId) -> ShareAcknowledgeCallEvidence {
    let (_request, evidence) =
        ShareAcknowledgeCallEvidence::from_prepared(broker_id, prepared_request());
    evidence
}

fn complete_response() -> ShareAcknowledgeResponse {
    let mut response = ShareAcknowledgeResponse::default();
    response.responses = vec![
        topic(1, vec![partition(0), partition(1)]),
        topic(2, vec![partition(0)]),
    ];
    response
}

fn topic(value: u8, partitions: Vec<PartitionData>) -> ShareAcknowledgeTopicResponse {
    let mut topic = ShareAcknowledgeTopicResponse::default();
    topic.topic_id = Uuid::from_bytes(id(value));
    topic.partitions = partitions;
    topic
}

fn partition(index: i32) -> PartitionData {
    let mut partition = PartitionData::default();
    partition.partition_index = index;
    partition.current_leader.leader_id = -1;
    partition.current_leader.leader_epoch = -1;
    partition
}

fn broker() -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("valid broker"))
}
