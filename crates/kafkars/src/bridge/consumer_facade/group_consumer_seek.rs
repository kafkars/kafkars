//! Capture-first translation and runtime-neutral observation for group seek.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    GroupConsumerPartition as EnginePartition, GroupConsumerPartitionInputError,
    GroupConsumerPartitionInputErrorKind, GroupConsumerSeek as EngineSeek,
    GroupConsumerSeekAdmissionErrorKind, GroupConsumerSeekError, GroupConsumerSeekErrorKind,
    GroupConsumerSeekPosition as EnginePosition,
};

use crate::{
    ErrorKind, KafkaError,
    consumer::{StartPosition, TopicPartition},
};

use super::group_consumer::GroupConsumerEngine;

/// Private named observation over one engine group seek.
pub(crate) struct GroupConsumerSeek<'consumer> {
    inner: GroupConsumerSeekInner<'consumer>,
}

enum GroupConsumerSeekInner<'consumer> {
    Engine(EngineSeek<'consumer>),
    Rejected(Option<KafkaError>),
}

impl GroupConsumerEngine {
    /// Captures the engine deadline before converting either facade value.
    pub(crate) fn seek(
        &mut self,
        partition: TopicPartition,
        position: StartPosition,
    ) -> GroupConsumerSeek<'_> {
        if let Some(error) = &self.startup_fault {
            return GroupConsumerSeek::rejected(error.clone());
        }
        let capture = match self.handle.capture_seek() {
            Ok(capture) => capture,
            Err(error) => {
                return GroupConsumerSeek::rejected(translate_admission_kind(error.kind()));
            }
        };
        let partition = match engine_partition(partition) {
            Ok(partition) => partition,
            Err(error) => return GroupConsumerSeek::rejected(error),
        };
        match capture.try_seek(partition, engine_position(position)) {
            Ok(seek) => GroupConsumerSeek::from_engine(seek),
            Err(error) => GroupConsumerSeek::rejected(translate_admission_kind(error.kind())),
        }
    }
}

impl<'consumer> GroupConsumerSeek<'consumer> {
    const fn from_engine(inner: EngineSeek<'consumer>) -> Self {
        Self {
            inner: GroupConsumerSeekInner::Engine(inner),
        }
    }

    pub(super) fn rejected(error: KafkaError) -> Self {
        Self {
            inner: GroupConsumerSeekInner::Rejected(Some(error)),
        }
    }

    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        match self.inner {
            GroupConsumerSeekInner::Engine(inner) => inner.wait().map_err(translate_terminal),
            GroupConsumerSeekInner::Rejected(mut error) => {
                Err(error.take().unwrap_or_else(observed_twice))
            }
        }
    }
}

impl Future for GroupConsumerSeek<'_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            GroupConsumerSeekInner::Engine(inner) => {
                Pin::new(inner).poll(context).map_err(translate_terminal)
            }
            GroupConsumerSeekInner::Rejected(error) => {
                Poll::Ready(Err(error.take().unwrap_or_else(observed_twice)))
            }
        }
    }
}

impl std::fmt::Debug for GroupConsumerSeek<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerSeek")
            .finish_non_exhaustive()
    }
}

pub(super) fn engine_partition(partition: TopicPartition) -> Result<EnginePartition, KafkaError> {
    let (topic, partition, start) = partition.into_parts();
    if start.is_some() {
        return Err(KafkaError::new(
            ErrorKind::Configuration,
            "group seek target cannot include a direct-assignment start position",
        ));
    }
    EnginePartition::try_new(topic, partition).map_err(translate_partition_input)
}

const fn engine_position(position: StartPosition) -> EnginePosition {
    match position {
        StartPosition::Beginning => EnginePosition::Beginning,
        StartPosition::End => EnginePosition::End,
        StartPosition::Offset(offset) => EnginePosition::Offset(offset),
    }
}

fn translate_partition_input(error: GroupConsumerPartitionInputError) -> KafkaError {
    let message = match error.kind() {
        GroupConsumerPartitionInputErrorKind::EmptyTopic => "group seek topic must not be empty",
        GroupConsumerPartitionInputErrorKind::TopicTooLong => {
            "group seek topic exceeds Kafka's length limit"
        }
        GroupConsumerPartitionInputErrorKind::NegativePartition => {
            "group seek partition must be nonnegative"
        }
    };
    KafkaError::new(ErrorKind::Configuration, message)
}

pub(super) fn translate_admission_kind(kind: GroupConsumerSeekAdmissionErrorKind) -> KafkaError {
    let (facade_kind, message) = match kind {
        GroupConsumerSeekAdmissionErrorKind::Contended
        | GroupConsumerSeekAdmissionErrorKind::Pending
        | GroupConsumerSeekAdmissionErrorKind::ResourceExhausted => (
            ErrorKind::Backpressure,
            "group seek admission is temporarily unavailable",
        ),
        GroupConsumerSeekAdmissionErrorKind::Closed
        | GroupConsumerSeekAdmissionErrorKind::GroupUnavailable
        | GroupConsumerSeekAdmissionErrorKind::NoActiveAssignment => {
            (ErrorKind::State, "group seek has no current assignment")
        }
        GroupConsumerSeekAdmissionErrorKind::UnknownPartition => (
            ErrorKind::State,
            "group seek partition is not in the current assignment",
        ),
        GroupConsumerSeekAdmissionErrorKind::NegativeOffset => (
            ErrorKind::Configuration,
            "group seek offset must be nonnegative",
        ),
        GroupConsumerSeekAdmissionErrorKind::HostUnavailable
        | GroupConsumerSeekAdmissionErrorKind::InternalInvariant => {
            (ErrorKind::Internal, "group seek ownership is unavailable")
        }
    };
    let error = KafkaError::new(facade_kind, message);
    match kind {
        GroupConsumerSeekAdmissionErrorKind::Contended
        | GroupConsumerSeekAdmissionErrorKind::Pending
        | GroupConsumerSeekAdmissionErrorKind::ResourceExhausted => error.with_safe_retry(),
        _ => error,
    }
}

fn translate_terminal(error: GroupConsumerSeekError) -> KafkaError {
    translate_terminal_kind(error.kind(), error.broker_code())
}

pub(super) fn translate_terminal_kind(
    kind: GroupConsumerSeekErrorKind,
    broker_code: Option<i16>,
) -> KafkaError {
    match kind {
        GroupConsumerSeekErrorKind::DeadlineElapsed => {
            KafkaError::new(ErrorKind::Timeout, "group seek deadline elapsed")
        }
        GroupConsumerSeekErrorKind::DriverRejected => KafkaError::new(
            ErrorKind::Backpressure,
            "group seek position lookup was rejected before driver admission",
        ),
        GroupConsumerSeekErrorKind::Transport => {
            KafkaError::new(ErrorKind::Transport, "group seek transport failed")
        }
        GroupConsumerSeekErrorKind::BrokerRejected => KafkaError::new(
            ErrorKind::Broker,
            "Kafka rejected group seek position resolution",
        )
        .with_broker_code(broker_code),
        GroupConsumerSeekErrorKind::Compatibility => KafkaError::new(
            ErrorKind::Compatibility,
            "no compatible group seek position protocol is available",
        ),
        GroupConsumerSeekErrorKind::AssignmentLost => {
            KafkaError::new(ErrorKind::State, "group seek assignment was lost")
        }
        GroupConsumerSeekErrorKind::InvalidResponse
        | GroupConsumerSeekErrorKind::ResponseTooLarge
        | GroupConsumerSeekErrorKind::HostUnavailable
        | GroupConsumerSeekErrorKind::InternalInvariant => KafkaError::new(
            ErrorKind::Internal,
            "group seek terminal ownership or response is inconsistent",
        ),
    }
}

fn observed_twice() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "group seek startup error was already observed",
    )
}
