//! Non-clone assigned-consumer calls with deadline-first lock acquisition.

use std::time::Duration;

use kafka_client_core::AssignmentEpoch;

use super::super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_model::AssignedConsumerOwnerError,
    assigned_topics::AssignedPartitionInput, fetch_store::FetchDelivery,
};
use super::{
    assignment::AssignedConsumerStartPosition,
    control::AssignedConsumerPartition,
    delivery::AssignedConsumerDelivery,
    reclaim::AssignedConsumerReclaimRejection,
    result::{AssignedConsumerAccepted, AssignedConsumerPortError},
    shard::AssignedConsumerPort,
};

impl AssignedConsumerPort {
    pub(crate) fn capture_assignment_deadline(
        &self,
        timeout: Duration,
    ) -> Result<crate::clock::DeadlineCapture, AssignedConsumerPortError> {
        self.shared
            .clock
            .capture_deadline_after(timeout)
            .map_err(AssignedConsumerPortError::Clock)
    }

    pub(crate) fn replace_assignment(
        &self,
        entries: Vec<AssignedPartitionInput>,
        timeout: Duration,
    ) -> Result<AssignedConsumerAccepted<AssignmentEpoch>, AssignedConsumerPortError> {
        let deadline = self.capture_assignment_deadline(timeout)?;
        self.replace_assignment_captured(entries, deadline)
    }

    pub(crate) fn replace_assignment_captured(
        &self,
        entries: Vec<AssignedPartitionInput>,
        deadline: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerAccepted<AssignmentEpoch>, AssignedConsumerPortError> {
        self.admit(move |owner| owner.replace_assignment_captured(entries, deadline))
    }

    pub(crate) fn capture_control_deadline(
        &self,
        timeout: Duration,
    ) -> Result<crate::clock::DeadlineCapture, AssignedConsumerPortError> {
        self.shared
            .clock
            .capture_deadline_after(timeout)
            .map_err(AssignedConsumerPortError::Clock)
    }

    pub(crate) fn pause(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedConsumerPartition,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        self.admit(move |owner| owner.pause_named(epoch, &partition))
    }

    pub(crate) fn resume(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedConsumerPartition,
        timeout: Duration,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        let capture = self.capture_control_deadline(timeout)?;
        self.resume_captured(epoch, partition, capture)
    }

    pub(crate) fn resume_captured(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedConsumerPartition,
        capture: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        self.admit(move |owner| owner.resume_named_captured(epoch, &partition, capture))
    }

    pub(crate) fn seek(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedConsumerPartition,
        position: AssignedConsumerStartPosition,
        timeout: Duration,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        let capture = self.capture_control_deadline(timeout)?;
        self.seek_captured(epoch, partition, position, capture)
    }

    pub(crate) fn seek_captured(
        &self,
        epoch: AssignmentEpoch,
        partition: AssignedConsumerPartition,
        position: AssignedConsumerStartPosition,
        capture: crate::clock::DeadlineCapture,
    ) -> Result<AssignedConsumerAccepted<()>, AssignedConsumerPortError> {
        self.admit(move |owner| owner.seek_named_captured(epoch, &partition, position, capture))
    }

    pub(crate) fn begin_close(
        &self,
    ) -> Result<
        AssignedConsumerAccepted<super::AssignedConsumerCloseObserver>,
        AssignedConsumerPortError,
    > {
        let result = self
            .shared
            .begin_assigned_close()
            .map_err(AssignedConsumerPortError::Lock)?;
        let Some(result) = result else {
            return Err(AssignedConsumerPortError::Closed);
        };
        let observer = self.finish_owner_result(result)?;
        Ok(AssignedConsumerAccepted::new(
            observer,
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

    pub(super) fn take_named_delivery(
        &self,
    ) -> Result<Option<AssignedConsumerDelivery>, AssignedConsumerPortError> {
        let result = self
            .shared
            .try_admit_with_owner(AssignedConsumerOwner::take_named_delivery)
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
