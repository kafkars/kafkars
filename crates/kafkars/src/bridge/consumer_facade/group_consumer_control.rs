//! Batch Fetch control and clone-shared shutdown translation for one hosted group.

use kafka_client_engine::{
    GroupConsumerControl as EngineGroupConsumerControl, GroupConsumerControlAccepted,
    GroupConsumerControlError, GroupConsumerControlErrorKind,
    GroupConsumerHandle as EngineGroupConsumerHandle,
    GroupConsumerPartition as EngineGroupConsumerPartition, GroupConsumerPartitionInputError,
    GroupConsumerPartitionInputErrorKind, GroupConsumerResumeCaptureError,
    GroupConsumerResumeCaptureErrorKind,
};

use crate::{ErrorKind, KafkaError, consumer::TopicPartition};

use super::group_consumer::GroupConsumerEngine;

/// Private cloneable bridge over one engine-owned group wakeup capability.
#[derive(Clone)]
pub(crate) struct GroupConsumerControl {
    inner: EngineGroupConsumerControl,
}

impl GroupConsumerControl {
    pub(super) const fn from_engine(inner: EngineGroupConsumerControl) -> Self {
        Self { inner }
    }

    pub(crate) fn request_shutdown(&self) {
        self.inner.request_shutdown();
    }
}

impl core::fmt::Debug for GroupConsumerControl {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerControl")
            .finish_non_exhaustive()
    }
}

impl GroupConsumerEngine {
    /// Clones the engine-owned group shutdown capability without sharing mutation.
    pub(crate) fn control(&self) -> GroupConsumerControl {
        GroupConsumerControl::from_engine(self.handle.control())
    }

    /// Pauses one fully validated batch of current assignment partitions.
    pub(crate) fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        control(
            &mut self.handle,
            self.startup_fault.as_ref(),
            partitions,
            EngineGroupConsumerHandle::pause,
        )
    }

    /// Resumes one fully validated batch under a capture-first deadline.
    pub(crate) fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        if partitions.is_empty() {
            return Ok(());
        }
        if let Some(error) = &self.startup_fault {
            return Err(error.clone());
        }
        let capture = self
            .handle
            .capture_resume()
            .map_err(translate_group_consumer_resume_capture)?;
        let partitions = engine_partitions(partitions)?;
        let _accepted = capture
            .resume(partitions)
            .map_err(|error| translate_group_consumer_control(&error))?;
        Ok(())
    }
}

fn control(
    handle: &mut EngineGroupConsumerHandle,
    startup_fault: Option<&KafkaError>,
    partitions: &[TopicPartition],
    admit: fn(
        &mut EngineGroupConsumerHandle,
        Vec<EngineGroupConsumerPartition>,
    ) -> Result<GroupConsumerControlAccepted, GroupConsumerControlError>,
) -> Result<(), KafkaError> {
    let partitions = engine_partitions(partitions)?;
    if partitions.is_empty() {
        return Ok(());
    }
    if let Some(error) = startup_fault {
        return Err(error.clone());
    }
    let _accepted =
        admit(handle, partitions).map_err(|error| translate_group_consumer_control(&error))?;
    Ok(())
}

pub(super) fn engine_partitions(
    partitions: &[TopicPartition],
) -> Result<Vec<EngineGroupConsumerPartition>, KafkaError> {
    let mut converted = Vec::new();
    converted.try_reserve_exact(partitions.len()).map_err(|_| {
        KafkaError::new(
            ErrorKind::Internal,
            "group-consumer control target allocation is unavailable",
        )
    })?;
    for partition in partitions {
        if partition.start_position().is_some() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "group-consumer control target cannot include a start position",
            ));
        }
        converted.push(
            EngineGroupConsumerPartition::try_new(
                partition.topic().to_owned(),
                partition.partition(),
            )
            .map_err(translate_group_consumer_partition_input)?,
        );
    }
    Ok(converted)
}

fn translate_group_consumer_partition_input(error: GroupConsumerPartitionInputError) -> KafkaError {
    let message = match error.kind() {
        GroupConsumerPartitionInputErrorKind::EmptyTopic => {
            "group-consumer control topic must not be empty"
        }
        GroupConsumerPartitionInputErrorKind::TopicTooLong => {
            "group-consumer control topic exceeds Kafka's length limit"
        }
        GroupConsumerPartitionInputErrorKind::NegativePartition => {
            "group-consumer control partition must be nonnegative"
        }
    };
    KafkaError::new(ErrorKind::Configuration, message)
}

fn translate_group_consumer_control(error: &GroupConsumerControlError) -> KafkaError {
    translate_group_consumer_control_kind(error.kind())
}

pub(super) fn translate_group_consumer_control_kind(
    kind: GroupConsumerControlErrorKind,
) -> KafkaError {
    let (facade_kind, message) = match kind {
        GroupConsumerControlErrorKind::Contended | GroupConsumerControlErrorKind::Pending => (
            ErrorKind::Backpressure,
            "group-consumer control is temporarily unavailable",
        ),
        GroupConsumerControlErrorKind::Closed
        | GroupConsumerControlErrorKind::GroupUnavailable
        | GroupConsumerControlErrorKind::NoAssignment => (
            ErrorKind::State,
            "group-consumer control has no current assignment",
        ),
        GroupConsumerControlErrorKind::UnknownPartition => (
            ErrorKind::State,
            "partition is not in the current group assignment",
        ),
        GroupConsumerControlErrorKind::PositionNotRetained => (
            ErrorKind::State,
            "partition has no retained group position to resume",
        ),
        GroupConsumerControlErrorKind::DuplicatePartition => (
            ErrorKind::Configuration,
            "group-consumer control targets contain a duplicate partition",
        ),
        GroupConsumerControlErrorKind::HostUnavailable
        | GroupConsumerControlErrorKind::ResourceExhausted
        | GroupConsumerControlErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "group-consumer control ownership is unavailable",
        ),
    };
    let error = KafkaError::new(facade_kind, message);
    match kind {
        GroupConsumerControlErrorKind::Contended | GroupConsumerControlErrorKind::Pending => {
            error.with_safe_retry()
        }
        _ => error,
    }
}

fn translate_group_consumer_resume_capture(error: GroupConsumerResumeCaptureError) -> KafkaError {
    translate_group_consumer_resume_capture_kind(error.kind())
}

pub(super) fn translate_group_consumer_resume_capture_kind(
    kind: GroupConsumerResumeCaptureErrorKind,
) -> KafkaError {
    match kind {
        GroupConsumerResumeCaptureErrorKind::HostUnavailable => KafkaError::new(
            ErrorKind::Internal,
            "group-consumer resume deadline capture is unavailable",
        ),
    }
}
