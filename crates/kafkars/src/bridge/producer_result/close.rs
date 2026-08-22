//! Exhaustive translation of producer close admission and terminal results.

use kafka_client_engine::{
    ProducerFlushError as EngineFlushError, ProducerFlushResult as EngineFlushResult,
    ProducerObserverError as EngineObserverError, ProducerTryCloseError as EngineTryCloseError,
    ProducerTryCloseErrorKind as EngineTryCloseErrorKind,
};

use crate::{ErrorKind, KafkaError};

use super::flush::admission_kind;

pub(crate) fn translate_close_admission(error: &EngineTryCloseError) -> KafkaError {
    let kind = error.kind();
    let Some(detail) = error.detail() else {
        return translate_close_admission_kind(kind);
    };
    close_admission(kind, detail.to_owned())
}

pub(super) fn translate_close_admission_kind(kind: EngineTryCloseErrorKind) -> KafkaError {
    close_admission(kind, close_message(kind).to_owned())
}

fn close_admission(kind: EngineTryCloseErrorKind, message: String) -> KafkaError {
    let error = KafkaError::new(admission_kind(kind), message);
    match kind {
        EngineTryCloseErrorKind::Contended | EngineTryCloseErrorKind::CompletionCapacity => {
            error.with_safe_retry()
        }
        EngineTryCloseErrorKind::MomentUnrepresentable
        | EngineTryCloseErrorKind::Closed
        | EngineTryCloseErrorKind::LocalIdentityExhausted
        | EngineTryCloseErrorKind::HostPoisoned
        | EngineTryCloseErrorKind::InternalInvariant => error,
    }
}

pub(crate) fn translate_close_result(result: EngineFlushResult) -> Result<(), KafkaError> {
    result.map_err(|error| match error {
        EngineFlushError::ExecutionUnavailable => KafkaError::new(
            ErrorKind::Internal,
            "producer execution owner became unavailable before close completed",
        ),
        EngineFlushError::Observer(observer) => observer_error(observer),
    })
}

const fn close_message(kind: EngineTryCloseErrorKind) -> &'static str {
    match kind {
        EngineTryCloseErrorKind::MomentUnrepresentable => {
            "producer close call boundary cannot be represented"
        }
        EngineTryCloseErrorKind::Contended => "producer close admission is contended",
        EngineTryCloseErrorKind::CompletionCapacity => "producer completion capacity is exhausted",
        EngineTryCloseErrorKind::Closed => "producer is already closed",
        EngineTryCloseErrorKind::LocalIdentityExhausted => {
            "producer close identity space is exhausted"
        }
        EngineTryCloseErrorKind::HostPoisoned => "producer host is unavailable",
        EngineTryCloseErrorKind::InternalInvariant => {
            "producer close admission violated an internal contract"
        }
    }
}

fn observer_error(error: EngineObserverError) -> KafkaError {
    match error {
        EngineObserverError::AlreadyObserved => {
            KafkaError::new(ErrorKind::State, "producer close was already observed")
        }
        EngineObserverError::Stale => {
            KafkaError::new(ErrorKind::State, "producer close observer is stale")
        }
        EngineObserverError::TerminalTypeMismatch => KafkaError::new(
            ErrorKind::Internal,
            "producer close observer received the wrong terminal type",
        ),
    }
}
