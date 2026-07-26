//! Deadline-first reservation and admission of one concrete Fetch execution.

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedConsumerTransition, FetchFailure,
    FetchOwnership, Moment,
};

use crate::driver::{DriverOwner, FetchCallAdmission, classify_fetch_admission};

use super::{
    super::fetch_store::FetchStoreFailure,
    executor::{ActiveFetchReservation, DirectFetchExecutor},
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
};

/// Result of one deadline-first bounded Fetch admission attempt.
#[must_use = "backpressured and unavailable Fetch ownership must be retained"]
pub(crate) enum FetchSubmission {
    Accepted,
    Backpressured(PreparedFetchExecution),
    Unavailable(PreparedFetchExecution),
    Settled(Option<AssignedConsumerTransition>),
}

impl DirectFetchExecutor {
    pub(crate) fn submit(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        prepared: PreparedFetchExecution,
        now: Moment,
    ) -> Result<FetchSubmission, FetchExecutionError> {
        if self.fault.is_some() {
            return Ok(FetchSubmission::Unavailable(prepared));
        }
        let prepared = match prepared.reconcile_ownership(machine) {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(FetchSubmission::Settled(None)),
            Err((error, prepared)) => {
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: prepared,
                });
                return Err(FetchExecutionError::Core(error));
            }
        };
        if prepared
            .request
            .operation_deadline()
            .core()
            .is_elapsed_at(now)
        {
            return self.settle_unadmitted(machine, prepared, FetchFailure::DeadlineElapsed);
        }
        if self.active.try_reserve(1).is_err() {
            return Ok(FetchSubmission::Backpressured(prepared));
        }
        let reservation = match self
            .store
            .try_reserve(prepared.fence(), prepared.hard_output_bytes)
        {
            Ok(reservation) => reservation,
            Err(
                FetchStoreFailure::CountCapacity
                | FetchStoreFailure::ByteCapacity
                | FetchStoreFailure::AccountingOverflow,
            ) => return Ok(FetchSubmission::Backpressured(prepared)),
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: prepared,
                });
                return Err(FetchExecutionError::Store(error));
            }
        };
        let fence = prepared.fence();
        let hard_output_bytes = prepared.hard_output_bytes;
        self.active
            .push(ActiveFetchReservation { fence, reservation });
        match self.calls.try_submit_fetch(driver, prepared.request, now) {
            FetchCallAdmission::Accepted => Ok(FetchSubmission::Accepted),
            FetchCallAdmission::Backpressured(request) => {
                let prepared = PreparedFetchExecution::from_parts(request, hard_output_bytes);
                let (prepared, reservation) = self.take_active_for_admission(prepared)?;
                self.rollback_or_fault(prepared, reservation)
                    .map(FetchSubmission::Backpressured)
            }
            FetchCallAdmission::Rejected(failure) => {
                let (request, source) = failure.into_parts();
                let prepared = PreparedFetchExecution::from_parts(request, hard_output_bytes);
                let failure = classify_fetch_admission(&source);
                let (prepared, reservation) = self.take_active_for_admission(prepared)?;
                let prepared = self.rollback_or_fault(prepared, reservation)?;
                self.settle_unadmitted(machine, prepared, failure)
            }
        }
    }

    fn take_active_for_admission(
        &mut self,
        prepared: PreparedFetchExecution,
    ) -> Result<
        (
            PreparedFetchExecution,
            super::super::fetch_store::FetchStoreReservation,
        ),
        FetchExecutionError,
    > {
        let fence = prepared.fence();
        let Some(index) = self.active_index(fence) else {
            self.fault = Some(RetainedFetchFault::Prepared {
                _prepared: prepared,
            });
            return Err(FetchExecutionError::MissingReservation { fence });
        };
        Ok((prepared, self.take_active(index).reservation))
    }

    fn rollback_or_fault(
        &mut self,
        prepared: PreparedFetchExecution,
        reservation: super::super::fetch_store::FetchStoreReservation,
    ) -> Result<PreparedFetchExecution, FetchExecutionError> {
        let (proof, output) = reservation.into_protocol_parts();
        match self.store.rollback(proof, output) {
            Ok(()) => Ok(prepared),
            Err((error, (proof, output))) => {
                self.fault = Some(RetainedFetchFault::PreparedRollback {
                    _prepared: prepared,
                    _proof: proof,
                    _output: output,
                });
                Err(FetchExecutionError::Store(error))
            }
        }
    }

    fn settle_unadmitted(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        prepared: PreparedFetchExecution,
        failure: FetchFailure,
    ) -> Result<FetchSubmission, FetchExecutionError> {
        let fence = prepared.fence();
        match machine.apply(AssignedConsumerInput::FetchFailed { fence, failure }) {
            Ok(transition) => Ok(FetchSubmission::Settled(Some(transition))),
            Err(_error)
                if matches!(
                    machine.fetch_ownership(fence),
                    Ok(FetchOwnership::Superseded)
                ) =>
            {
                Ok(FetchSubmission::Settled(None))
            }
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: prepared,
                });
                Err(FetchExecutionError::Core(error))
            }
        }
    }
}
