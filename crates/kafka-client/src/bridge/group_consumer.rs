//! Private facade translation for bounded classic-group registration ownership.

use kafka_client_engine::{
    GroupConsumerHandle as EngineGroupConsumerHandle, GroupConsumerTryTakeBatchErrorKind,
};

use crate::bridge::consumer_facade::group_consumer_event::{
    translate_group_consumer_state, translate_group_consumer_state_error,
};
use crate::bridge::consumer_facade::group_consumer_recv_result::{
    translate_group_consumer_fetch_failure, translate_group_consumer_position_failure,
};
use crate::bridge::consumer_facade::{
    group_consumer_batch::GroupConsumerBatch, group_consumer_recv::GroupConsumerRecv,
};
use crate::{ErrorKind, KafkaError};

/// Private linear bridge retaining one registered engine group owner.
pub(crate) struct GroupConsumerEngine {
    pub(super) handle: EngineGroupConsumerHandle,
    pub(super) startup_fault: Option<KafkaError>,
}

impl GroupConsumerEngine {
    pub(crate) fn startup_fault(&self) -> Option<KafkaError> {
        self.startup_fault.clone()
    }

    /// Copies one atomically confirmed membership and assignment without group work.
    pub(crate) fn state(
        &self,
    ) -> Result<
        Option<(
            crate::consumer::ConsumerAssignment,
            crate::consumer::GroupMetadata,
        )>,
        KafkaError,
    > {
        if let Some(error) = &self.startup_fault {
            return Err(error.clone());
        }
        self.handle
            .state()
            .map(|state| state.map(translate_group_consumer_state))
            .map_err(translate_group_consumer_state_error)
    }

    /// Observes one already-authorized delivery without starting Fetch work.
    pub(crate) fn recv(&mut self) -> GroupConsumerRecv<'_> {
        match &self.startup_fault {
            Some(error) => GroupConsumerRecv::rejected(error.clone()),
            None => GroupConsumerRecv::from_engine(self.handle.recv()),
        }
    }

    /// Transfers one already-authorized delivery without starting Fetch work.
    pub(crate) fn try_take_batch(&mut self) -> Result<Option<GroupConsumerBatch>, KafkaError> {
        if let Some(error) = &self.startup_fault {
            return Err(error.clone());
        }
        self.handle
            .try_take_batch()
            .map(|batch| batch.map(GroupConsumerBatch::from_engine))
            .map_err(|error| match error.kind() {
                GroupConsumerTryTakeBatchErrorKind::Contended
                | GroupConsumerTryTakeBatchErrorKind::Pending => KafkaError::new(
                    ErrorKind::Backpressure,
                    "group delivery is temporarily unavailable",
                ),
                GroupConsumerTryTakeBatchErrorKind::Closed
                | GroupConsumerTryTakeBatchErrorKind::GroupUnavailable => {
                    KafkaError::new(ErrorKind::State, "group delivery admission is closed")
                }
                GroupConsumerTryTakeBatchErrorKind::ProcessingExpired => KafkaError::new(
                    ErrorKind::State,
                    "group application-processing lease expired",
                ),
                GroupConsumerTryTakeBatchErrorKind::Position(failure) => {
                    translate_group_consumer_position_failure(failure)
                }
                GroupConsumerTryTakeBatchErrorKind::Fetch(failure) => {
                    translate_group_consumer_fetch_failure(failure)
                }
                GroupConsumerTryTakeBatchErrorKind::HostUnavailable
                | GroupConsumerTryTakeBatchErrorKind::InternalInvariant => KafkaError::new(
                    ErrorKind::Internal,
                    "group delivery ownership is unavailable",
                ),
            })
    }
}

impl core::fmt::Debug for GroupConsumerEngine {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerEngine")
            .field("handle", &self.handle)
            .field("startup_fault", &self.startup_fault)
            .finish_non_exhaustive()
    }
}
