//! Exhaustive producer flush admission and terminal translation scenarios.

use kafka_client_engine::{
    ProducerFlushError as EngineFlushError, ProducerObserverError as EngineObserverError,
    ProducerTryFlushErrorKind as EngineTryFlushErrorKind,
};

use super::flush::{admission_kind, translate_flush_result};
use crate::ErrorKind;

#[test]
fn every_flush_admission_kind_has_one_stable_facade_category() {
    let cases = [
        (
            EngineTryFlushErrorKind::MomentUnrepresentable,
            ErrorKind::Internal,
        ),
        (EngineTryFlushErrorKind::Contended, ErrorKind::Backpressure),
        (
            EngineTryFlushErrorKind::CompletionCapacity,
            ErrorKind::Backpressure,
        ),
        (EngineTryFlushErrorKind::Closed, ErrorKind::State),
        (
            EngineTryFlushErrorKind::LocalIdentityExhausted,
            ErrorKind::Internal,
        ),
        (EngineTryFlushErrorKind::HostPoisoned, ErrorKind::Internal),
        (
            EngineTryFlushErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ];

    for (engine, facade) in cases {
        assert_eq!(admission_kind(engine), facade);
    }
}

#[test]
fn terminal_type_mismatch_is_an_internal_contract_failure() {
    let result = translate_flush_result(Err(EngineFlushError::Observer(
        EngineObserverError::TerminalTypeMismatch,
    )));
    let Err(error) = result else {
        panic!("wrong terminal type must fail flush observation")
    };

    assert_eq!(error.kind(), ErrorKind::Internal);
}
