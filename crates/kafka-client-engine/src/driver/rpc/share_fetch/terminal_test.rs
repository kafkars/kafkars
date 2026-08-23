//! Terminal response, broker rejection, and delivery-certainty evidence.

use kafka_client_core::{DeliveryStatus, Moment, ShareFetchBrokerId};
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::ShareFetchResponse;

use crate::protocol::consumer::share_fetch::{
    PreparedShareFetchRequest, ShareFetchRequestPlan, ShareFetchRequestSettings,
    ShareFetchRequestTopic, ShareFetchResponseLimits, share_fetch_request,
};

use super::{
    ShareFetchFailureKind, ShareFetchResolution, call::ShareFetchCallEvidence,
    terminal::retain_share_fetch_terminal,
};

#[test]
fn terminal_normalizes_success_and_preserves_submission_context() {
    let broker = broker();
    let submitted_at = Moment::from_tick(17);
    let mut response = ShareFetchResponse::default();
    response.acquisition_lock_timeout_ms = 30_000;
    let terminal = retain_share_fetch_terminal(
        evidence(broker, submitted_at),
        Some(ApiVersion::new(1)),
        Ok(response),
        None,
    );
    let (resolution, route, context) =
        terminal.into_resolution(ShareFetchResponseLimits::new(8, 16));
    let ShareFetchResolution::Succeeded(success) = resolution else {
        panic!("success expected");
    };
    assert_eq!(success.acquisition_lock_timeout_ms, Some(30_000));
    assert_eq!(route.broker_id(), broker);
    route.accept();
    assert_eq!(context.broker_id, broker);
    assert_eq!(context.submitted_at, submitted_at);
}

#[test]
fn terminal_preserves_broker_code_and_request_delivery_certainty() {
    let mut rejected = ShareFetchResponse::default();
    rejected.error_code = 16;
    let terminal = retain_share_fetch_terminal(
        evidence(broker(), Moment::from_tick(1)),
        Some(ApiVersion::new(1)),
        Ok(rejected),
        None,
    );
    let (resolution, route, _context) =
        terminal.into_resolution(ShareFetchResponseLimits::new(1, 1));
    let ShareFetchResolution::BrokerRejected(rejection) = resolution else {
        panic!("broker rejection expected");
    };
    assert_eq!(rejection.error_code.get(), 16);
    route.accept();

    let terminal = retain_share_fetch_terminal(
        evidence(broker(), Moment::from_tick(1)),
        Some(ApiVersion::new(1)),
        Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }),
        None,
    );
    let (resolution, route, _context) =
        terminal.into_resolution(ShareFetchResponseLimits::new(1, 1));
    assert_eq!(
        resolution,
        ShareFetchResolution::Failed {
            kind: ShareFetchFailureKind::DeadlineElapsed,
            delivery: DeliveryStatus::NotSent,
        }
    );
    route.accept();
}

#[test]
fn missing_version_and_malformed_success_fail_with_possibly_sent_certainty() {
    let mut response = ShareFetchResponse::default();
    response.acquisition_lock_timeout_ms = 30_000;
    let terminal = retain_share_fetch_terminal(
        evidence(broker(), Moment::from_tick(1)),
        None,
        Ok(response),
        None,
    );
    let (resolution, route, _context) =
        terminal.into_resolution(ShareFetchResponseLimits::new(1, 1));
    assert_eq!(
        resolution,
        ShareFetchResolution::Failed {
            kind: ShareFetchFailureKind::Compatibility,
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    route.accept();

    let malformed = ShareFetchResponse::default();
    let terminal = retain_share_fetch_terminal(
        evidence(broker(), Moment::from_tick(1)),
        Some(ApiVersion::new(1)),
        Ok(malformed),
        None,
    );
    let (resolution, route, _context) =
        terminal.into_resolution(ShareFetchResponseLimits::new(1, 1));
    assert_eq!(
        resolution,
        ShareFetchResolution::Failed {
            kind: ShareFetchFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    route.accept();
}

pub(super) fn prepared() -> PreparedShareFetchRequest {
    share_fetch_request(
        "workers",
        "member-a",
        0,
        ShareFetchRequestSettings {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_024,
            max_records: 8,
            batch_size: 4,
        },
        ShareFetchRequestPlan::try_new(vec![topic()], vec![topic()], vec![])
            .unwrap_or_else(|error| panic!("valid plan: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("valid request: {error:?}"))
}

fn topic() -> ShareFetchRequestTopic {
    ShareFetchRequestTopic::try_new(topic_id(), vec![0])
        .unwrap_or_else(|error| panic!("valid topic: {error:?}"))
}

fn topic_id() -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = 1;
    id
}

fn broker() -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("valid broker"))
}

fn evidence(broker_id: ShareFetchBrokerId, submitted_at: Moment) -> ShareFetchCallEvidence {
    let (_request, evidence) =
        ShareFetchCallEvidence::from_prepared(broker_id, submitted_at, prepared());
    evidence
}
