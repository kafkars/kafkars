//! Producer close admission and terminal translation scenarios.

use kafka_client_engine::{
    ProducerFlushError as EngineFlushError, ProducerObserverError as EngineObserverError,
    ProducerTryCloseErrorKind as EngineTryCloseErrorKind,
};

use super::{close::translate_close_result, flush::admission_kind};
use crate::ErrorKind;

#[test]
fn every_close_admission_kind_has_one_stable_facade_category() {
    let cases = [
        (
            EngineTryCloseErrorKind::MomentUnrepresentable,
            ErrorKind::Internal,
        ),
        (EngineTryCloseErrorKind::Contended, ErrorKind::Backpressure),
        (
            EngineTryCloseErrorKind::CompletionCapacity,
            ErrorKind::Backpressure,
        ),
        (EngineTryCloseErrorKind::Closed, ErrorKind::State),
        (
            EngineTryCloseErrorKind::LocalIdentityExhausted,
            ErrorKind::Internal,
        ),
        (EngineTryCloseErrorKind::HostPoisoned, ErrorKind::Internal),
        (
            EngineTryCloseErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ];

    for (engine, facade) in cases {
        assert_eq!(admission_kind(engine), facade);
    }
}

#[test]
fn close_terminal_errors_never_invent_delivery_certainty() {
    let result = translate_close_result(Err(EngineFlushError::Observer(
        EngineObserverError::TerminalTypeMismatch,
    )));
    let Err(error) = result else {
        panic!("wrong terminal type must fail close observation")
    };

    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.delivery_status(), None);
}
