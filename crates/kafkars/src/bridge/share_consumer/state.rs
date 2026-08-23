//! Exact facade projection of immediate share membership state.

use kafka_client_engine::share::{ShareConsumerStartupFailureKind, ShareConsumerStateErrorKind};

use crate::{ErrorKind, KafkaError, consumer::ShareConsumerAssignment};

use super::registration::ShareConsumerEngine;

impl ShareConsumerEngine {
    pub(crate) fn startup_fault(&self) -> Option<KafkaError> {
        self.startup_fault
            .clone()
            .or_else(|| self.handle.startup_failure().map(translate_startup_failure))
    }

    pub(crate) fn state(&self) -> Result<Option<ShareConsumerAssignment>, KafkaError> {
        if let Some(error) = &self.startup_fault {
            return Err(error.clone());
        }
        let state = self
            .handle
            .state()
            .map_err(|error| translate_state_error(error.kind()))?;
        let Some(state) = state else {
            return Ok(None);
        };
        let partitions = state
            .partitions()
            .iter()
            .map(|partition| {
                let partition_id = i32::try_from(partition.partition()).map_err(|_error| {
                    KafkaError::new(
                        ErrorKind::Internal,
                        "share assignment contains an invalid Kafka partition",
                    )
                })?;
                Ok(
                    crate::consumer::ShareConsumerAssignmentPartition::from_parts(
                        partition.topic().to_owned(),
                        partition_id,
                    ),
                )
            })
            .collect::<Result<Vec<_>, KafkaError>>()?;
        Ok(Some(ShareConsumerAssignment::from_parts(
            state.member_epoch(),
            state.assignment_generation(),
            partitions,
        )))
    }
}

pub(super) fn translate_state_error(kind: ShareConsumerStateErrorKind) -> KafkaError {
    match kind {
        ShareConsumerStateErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "share membership observation is temporarily contended",
        )
        .with_safe_retry(),
        ShareConsumerStateErrorKind::Allocation => KafkaError::new(
            ErrorKind::Backpressure,
            "share assignment observation exceeded local allocation capacity",
        )
        .with_safe_retry(),
        ShareConsumerStateErrorKind::Unavailable => {
            KafkaError::new(ErrorKind::State, "share membership is unavailable")
        }
        ShareConsumerStateErrorKind::Internal => KafkaError::new(
            ErrorKind::Internal,
            "share membership observation violated an ownership invariant",
        ),
    }
}

pub(super) fn translate_startup_failure(kind: ShareConsumerStartupFailureKind) -> KafkaError {
    let error = match kind {
        ShareConsumerStartupFailureKind::CoordinatorUnavailable => KafkaError::new(
            ErrorKind::Routing,
            "share-group coordinator remained unavailable",
        ),
        ShareConsumerStartupFailureKind::Compatibility => KafkaError::new(
            ErrorKind::Compatibility,
            "share-group protocol is incompatible with the broker",
        ),
        ShareConsumerStartupFailureKind::Execution => {
            KafkaError::new(ErrorKind::Internal, "share-group startup execution failed")
        }
        ShareConsumerStartupFailureKind::Broker(code) => KafkaError::new(
            ErrorKind::Broker,
            "share-group startup was rejected by the broker",
        )
        .with_broker_code(Some(code)),
        ShareConsumerStartupFailureKind::InvalidResponse => KafkaError::new(
            ErrorKind::Internal,
            "share-group startup received an invalid broker response",
        ),
        ShareConsumerStartupFailureKind::DeadlineElapsed => {
            KafkaError::new(ErrorKind::Timeout, "share-group startup deadline elapsed")
        }
    };
    error.with_fatal_disposition()
}
