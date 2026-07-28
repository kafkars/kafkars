//! Linear bridge ownership for one accepted transactional offset transfer.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use kafka_client_engine::TransactionOffsetsObserver as EngineObserver;

use crate::{
    KafkaError,
    bridge::consumer_facade::group_consumer_checkpoint::GroupConsumerCheckpoint,
    consumer::{Checkpoint, GroupMetadata},
};

use super::{
    TransactionEngine,
    offsets_result::{translate_admission, translate_observation},
};

impl<'producer> TransactionEngine<'producer> {
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection returns the exact metadata and checkpoint owners"
    )]
    pub(crate) fn send_offsets<'send>(
        &'send mut self,
        metadata: GroupMetadata,
        checkpoint: Checkpoint,
        timeout: Duration,
    ) -> Result<TransactionOffsetsEngine<'send, 'producer>, (GroupMetadata, Checkpoint, KafkaError)>
    {
        let Some(capture) = self.inner.capture_offsets(timeout) else {
            return Err((
                metadata,
                checkpoint,
                translate_admission(
                    kafka_client_engine::TransactionOffsetsAdmissionErrorKind::InvalidDeadline,
                ),
            ));
        };
        let Some(engine_metadata) = metadata.bridge_clone() else {
            return Err((
                metadata,
                checkpoint,
                KafkaError::new(
                    crate::ErrorKind::State,
                    "group metadata has no live assignment fence",
                )
                .with_delivery_status(crate::DeliveryStatus::NotSent),
            ));
        };
        let engine_checkpoint = checkpoint.into_bridge().into_engine();
        match self.inner.send_offsets_captured(
            engine_metadata.into_engine(),
            engine_checkpoint,
            capture,
        ) {
            Ok(accepted) => {
                let wake_failed = accepted.wake_failed();
                Ok(TransactionOffsetsEngine {
                    inner: accepted.into_observer(),
                    wake_failed,
                })
            }
            Err(error) => {
                let semantic = translate_admission(error.kind());
                let (_engine_metadata, checkpoint) = error.into_parts();
                Err((
                    metadata,
                    Checkpoint::from_bridge(GroupConsumerCheckpoint::from_engine(checkpoint)),
                    semantic,
                ))
            }
        }
    }
}

/// Private observer retaining both transaction and producer-owner borrows.
pub(crate) struct TransactionOffsetsEngine<'send, 'producer> {
    inner: EngineObserver<'send, 'producer>,
    wake_failed: bool,
}

impl TransactionOffsetsEngine<'_, '_> {
    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        translate_observation(self.inner.wait())
    }
}

impl Future for TransactionOffsetsEngine<'_, '_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(translate_observation)
    }
}

impl core::fmt::Debug for TransactionOffsetsEngine<'_, '_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionOffsetsEngine")
            .field("inner", &self.inner)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}
