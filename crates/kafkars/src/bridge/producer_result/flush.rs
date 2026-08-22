//! Exhaustive translation of producer flush admission and terminal results.

use kafka_client_engine::{
    ProducerFlushError as EngineFlushError, ProducerFlushResult as EngineFlushResult,
    ProducerObserverError as EngineObserverError, ProducerTryFlushError as EngineTryFlushError,
    ProducerTryFlushErrorKind as EngineTryFlushErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(crate) fn translate_flush_admission(error: &EngineTryFlushError) -> KafkaError {
    let kind = error.kind();
    let Some(detail) = error.detail() else {
        return translate_flush_admission_kind(kind);
    };
    flush_admission(kind, detail.to_owned())
}

pub(super) fn translate_flush_admission_kind(kind: EngineTryFlushErrorKind) -> KafkaError {
    flush_admission(kind, admission_message(kind).to_owned())
}

fn flush_admission(kind: EngineTryFlushErrorKind, message: String) -> KafkaError {
    let error = KafkaError::new(admission_kind(kind), message);
    match kind {
        EngineTryFlushErrorKind::Contended | EngineTryFlushErrorKind::CompletionCapacity => {
            error.with_safe_retry()
        }
        EngineTryFlushErrorKind::MomentUnrepresentable
        | EngineTryFlushErrorKind::Closed
        | EngineTryFlushErrorKind::LocalIdentityExhausted
        | EngineTryFlushErrorKind::HostPoisoned
        | EngineTryFlushErrorKind::InternalInvariant => error,
    }
}

pub(crate) fn translate_flush_result(result: EngineFlushResult) -> Result<(), KafkaError> {
    result.map_err(translate_flush_error)
}

fn translate_flush_error(error: EngineFlushError) -> KafkaError {
    match error {
        EngineFlushError::ExecutionUnavailable => KafkaError::new(
            ErrorKind::Internal,
            "producer execution owner became unavailable before flush completed",
        ),
        EngineFlushError::Observer(observer) => observer_error(observer),
    }
}

pub(super) const fn admission_kind(kind: EngineTryFlushErrorKind) -> ErrorKind {
    match kind {
        EngineTryFlushErrorKind::Contended | EngineTryFlushErrorKind::CompletionCapacity => {
            ErrorKind::Backpressure
        }
        EngineTryFlushErrorKind::Closed => ErrorKind::State,
        EngineTryFlushErrorKind::MomentUnrepresentable
        | EngineTryFlushErrorKind::LocalIdentityExhausted
        | EngineTryFlushErrorKind::HostPoisoned
        | EngineTryFlushErrorKind::InternalInvariant => ErrorKind::Internal,
    }
}

const fn admission_message(kind: EngineTryFlushErrorKind) -> &'static str {
    match kind {
        EngineTryFlushErrorKind::MomentUnrepresentable => {
            "producer flush call boundary cannot be represented"
        }
        EngineTryFlushErrorKind::Contended => "producer flush admission is contended",
        EngineTryFlushErrorKind::CompletionCapacity => "producer completion capacity is exhausted",
        EngineTryFlushErrorKind::Closed => "producer admission is closed",
        EngineTryFlushErrorKind::LocalIdentityExhausted => {
            "producer flush identity space is exhausted"
        }
        EngineTryFlushErrorKind::HostPoisoned => "producer host is unavailable",
        EngineTryFlushErrorKind::InternalInvariant => {
            "producer flush admission violated an internal contract"
        }
    }
}

fn observer_error(error: EngineObserverError) -> KafkaError {
    match error {
        EngineObserverError::AlreadyObserved => {
            KafkaError::new(ErrorKind::State, "producer flush was already observed")
        }
        EngineObserverError::Stale => {
            KafkaError::new(ErrorKind::State, "producer flush observer is stale")
        }
        EngineObserverError::TerminalTypeMismatch => KafkaError::new(
            ErrorKind::Internal,
            "producer flush observer received the wrong terminal type",
        ),
    }
}
