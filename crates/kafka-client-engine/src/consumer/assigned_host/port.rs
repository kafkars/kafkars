//! Non-clone assigned-consumer calls with deadline-first lock acquisition.

use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerCloseId, AssignedTopicPartition, AssignmentEpoch, StartPosition,
};

use super::super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_model::AssignedConsumerOwnerError,
    assigned_topics::AssignedPartitionInput, fetch_store::FetchDelivery,
};
use super::{
    reclaim::AssignedConsumerReclaimRejection,
    result::{AssignedConsumerAccepted, AssignedConsumerPortError},
    shard::AssignedConsumerPort,
};

impl AssignedConsumerPort {
    pub(crate) fn replace_assignment(
        &self,
        entries: Vec<AssignedPartitionInput>,
        timeout: Duration,
    ) -> Result<AssignedConsumerAccepted<AssignmentEpoch>, AssignedConsumerPortError> {
        let capture = self
            .shared
            .clock
            .capture_deadline_after(timeout)
            .map_err(AssignedConsumerPortError::Clock)?;
        self.admit(move |owner| owner.replace_assignment_captured(entries, capture))
    }

    pub(crate) fn pause(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        self.admit(move |owner| owner.pause(epoch, partition))
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the private assigned-consumer port precedes its facade claim seam"
        )
    )]
    pub(crate) fn resume(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        timeout: Duration,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        let capture = self
            .shared
            .clock
            .capture_deadline_after(timeout)
            .map_err(AssignedConsumerPortError::Clock)?;
        self.admit(move |owner| owner.resume_captured(epoch, partition, capture))
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the private assigned-consumer port precedes its facade claim seam"
        )
    )]
    pub(crate) fn seek(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        position: StartPosition,
        timeout: Duration,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        let capture = self
            .shared
            .clock
            .capture_deadline_after(timeout)
            .map_err(AssignedConsumerPortError::Clock)?;
        self.admit(move |owner| owner.seek_captured(epoch, partition, position, capture))
    }

    pub(crate) fn begin_close(
        &self,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        let result = self
            .shared
            .begin_assigned_close()
            .map_err(AssignedConsumerPortError::Lock)?;
        let Some(result) = result else {
            return Err(AssignedConsumerPortError::Closed);
        };
        self.finish_owner_result(result)?;
        Ok(AssignedConsumerAccepted::new(
            (),
            self.shared.wake.request_assigned_turn(),
        ))
    }

    pub(crate) fn take_delivery(&self) -> Result<Option<FetchDelivery>, AssignedConsumerPortError> {
        let result = self
            .shared
            .try_admit_with_owner(AssignedConsumerOwner::take_delivery)
            .map_err(AssignedConsumerPortError::Lock)?;
        let Some(result) = result else {
            return Err(AssignedConsumerPortError::Closed);
        };
        self.finish_owner_result(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "pre-transfer rejection must return the exact linear delivery without allocation"
    )]
    pub(crate) fn reclaim_delivery(
        &self,
        delivery: FetchDelivery,
    ) -> Result<
        AssignedConsumerAccepted<Result<(), AssignedConsumerOwnerError>>,
        AssignedConsumerReclaimRejection,
    > {
        let result = self.shared.reclaim_assigned_delivery(delivery)?;
        Ok(AssignedConsumerAccepted::new(
            result,
            self.shared.wake.request_assigned_turn(),
        ))
    }

    pub(crate) fn take_close(&self) -> Result<AssignedConsumerCloseId, AssignedConsumerPortError> {
        let result = self
            .shared
            .try_with_owner(AssignedConsumerOwner::take_close)
            .map_err(AssignedConsumerPortError::Lock)?;
        self.finish_owner_result(result)
    }

    fn admit<T>(
        &self,
        operation: impl FnOnce(&mut AssignedConsumerOwner) -> Result<T, AssignedConsumerOwnerError>,
    ) -> Result<AssignedConsumerAccepted<T>, AssignedConsumerPortError> {
        if self.shared.assigned_admission_is_closed() {
            return Err(AssignedConsumerPortError::Closed);
        }
        let value = self
            .shared
            .try_admit_with_owner(operation)
            .map_err(AssignedConsumerPortError::Lock)?;
        let Some(value) = value else {
            return Err(AssignedConsumerPortError::Closed);
        };
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                let wake = (error == AssignedConsumerOwnerError::Faulted)
                    .then(|| self.shared.wake.request_assigned_turn().err())
                    .flatten();
                return Err(AssignedConsumerPortError::Owner { error, wake });
            }
        };
        Ok(AssignedConsumerAccepted::new(
            value,
            self.shared.wake.request_assigned_turn(),
        ))
    }

    fn finish_owner_result<T>(
        &self,
        result: Result<T, AssignedConsumerOwnerError>,
    ) -> Result<T, AssignedConsumerPortError> {
        result.map_err(|error| AssignedConsumerPortError::Owner {
            error,
            wake: (error == AssignedConsumerOwnerError::Faulted)
                .then(|| self.shared.wake.request_assigned_turn().err())
                .flatten(),
        })
    }
}
