//! Producer identity terminal normalization scenarios.

use kafka_client_core::{Moment, ProducerIdentityGeneration, ProducerInput};
use kafka_driver::{CallFailure, Delivery, RequestError};
use kafka_wire::InitProducerIdResponse;

use super::init_producer_id_calls::normalize_terminal;

#[test]
fn success_carries_generation_identity_and_observed_moment() {
    let mut response = InitProducerIdResponse::default();
    response.producer_id = 44;
    response.producer_epoch = 3;
    assert_eq!(
        normalize_terminal(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(9),
            Ok(response),
        ),
        ProducerInput::ProducerIdentityAcquired {
            generation: ProducerIdentityGeneration::initial(),
            producer_id: 44,
            producer_epoch: 3,
            now: Moment::from_tick(9),
        }
    );
}

#[test]
fn broker_failure_preserves_exact_signed_code() {
    let mut response = InitProducerIdResponse::default();
    response.error_code = -47;
    assert!(matches!(
        normalize_terminal(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(9),
            Ok(response),
        ),
        ProducerInput::ProducerIdentityFailed {
            generation,
            broker_code: Some(code),
            now,
        } if generation == ProducerIdentityGeneration::initial()
            && code.get() == -47
            && now == Moment::from_tick(9)
    ));
}

#[test]
fn invalid_success_fields_do_not_create_an_identity() {
    let response = InitProducerIdResponse::default();
    assert_eq!(
        normalize_terminal(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(9),
            Ok(response),
        ),
        ProducerInput::ProducerIdentityFailed {
            generation: ProducerIdentityGeneration::initial(),
            broker_code: None,
            now: Moment::from_tick(9),
        }
    );
}

#[test]
fn driver_deadline_remains_an_explicit_core_fact() {
    assert_eq!(
        normalize_terminal(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(9),
            Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            }),
        ),
        ProducerInput::ProducerIdentityDeadlineElapsed {
            generation: ProducerIdentityGeneration::initial(),
            now: Moment::from_tick(9),
        }
    );
}

#[test]
fn non_deadline_request_failure_remains_an_explicit_core_fact() {
    assert_eq!(
        normalize_terminal(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(9),
            Err(RequestError::Rejected {
                failure: CallFailure::LocallyRejected,
                delivery: Delivery::NotSent,
            }),
        ),
        ProducerInput::ProducerIdentityRequestFailed {
            generation: ProducerIdentityGeneration::initial(),
            now: Moment::from_tick(9),
        }
    );
}
