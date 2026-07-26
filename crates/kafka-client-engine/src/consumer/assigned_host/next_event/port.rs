//! Atomic retained-event probe and wait registration under the owner lock.

use std::task::{Context, Poll};

use crate::consumer::assigned_host::{
    AssignedConsumerEvent, AssignedConsumerPort, AssignedConsumerShardLockError,
    event::translate_retained_event,
};

use super::{
    AssignedConsumerEventRegistration, AssignedConsumerNextEventError,
    signal::{AssignedConsumerEventSignalError, AssignedConsumerEventWait},
};

impl AssignedConsumerPort {
    pub(super) fn poll_next_event(
        &self,
        registration: &mut Option<AssignedConsumerEventRegistration>,
        context: &Context<'_>,
    ) -> Poll<Result<Option<AssignedConsumerEvent>, AssignedConsumerNextEventError>> {
        for attempt in 0..2 {
            match self.shared.try_owner() {
                Ok(mut guard) => {
                    let result = self.probe_next_event(&mut guard, registration, Some(context));
                    drop(guard);
                    self.shared.request_observation_unlock_notifications();
                    return result;
                }
                Err(AssignedConsumerShardLockError::Contended) => {
                    if let Err(error) = self.arm_event_task(
                        registration,
                        AssignedConsumerEventWait::Unlock,
                        context,
                    ) {
                        return Poll::Ready(Err(error));
                    }
                    if attempt == 1 {
                        return Poll::Pending;
                    }
                }
                Err(
                    AssignedConsumerShardLockError::Poisoned
                    | AssignedConsumerShardLockError::OwnerMissing,
                ) => {
                    self.finish_next_event(registration);
                    return Poll::Ready(Err(AssignedConsumerNextEventError::host_unavailable()));
                }
            }
        }
        Poll::Pending
    }

    pub(super) fn wait_next_event(
        &self,
        registration: &mut Option<AssignedConsumerEventRegistration>,
    ) -> Result<Option<AssignedConsumerEvent>, AssignedConsumerNextEventError> {
        loop {
            let mut guard = self
                .shared
                .owner()
                .map_err(|_error| AssignedConsumerNextEventError::host_unavailable())?;
            match self.probe_next_event(&mut guard, registration, None) {
                Poll::Ready(result) => {
                    drop(guard);
                    self.shared.request_observation_unlock_notifications();
                    return result;
                }
                Poll::Pending => {
                    let Some(id) = *registration else {
                        return Err(AssignedConsumerNextEventError::internal_invariant());
                    };
                    drop(guard);
                    self.shared.request_observation_unlock_notifications();
                    self.shared
                        .event_signal
                        .wait(id)
                        .map_err(translate_signal)?;
                }
            }
        }
    }

    pub(super) fn cancel_next_event(
        &self,
        registration: &mut Option<AssignedConsumerEventRegistration>,
    ) {
        if let Some(id) = registration.take() {
            self.shared.event_signal.cancel(id);
        }
    }

    fn probe_next_event(
        &self,
        guard: &mut Option<crate::consumer::AssignedConsumerOwner>,
        registration: &mut Option<AssignedConsumerEventRegistration>,
        context: Option<&Context<'_>>,
    ) -> Poll<Result<Option<AssignedConsumerEvent>, AssignedConsumerNextEventError>> {
        let Some(owner) = guard.as_mut() else {
            self.finish_next_event(registration);
            return Poll::Ready(Err(AssignedConsumerNextEventError::host_unavailable()));
        };
        if let Some(event) = owner.take_event() {
            self.finish_next_event(registration);
            return Poll::Ready(Ok(Some(translate_retained_event(event))));
        }
        if owner.fault_kind().is_some() {
            self.finish_next_event(registration);
            return Poll::Ready(Err(AssignedConsumerNextEventError::host_unavailable()));
        }
        if self.shared.assigned_admission_is_closed() {
            self.finish_next_event(registration);
            return Poll::Ready(Ok(None));
        }
        let armed = match context {
            Some(context) => {
                self.arm_event_task(registration, AssignedConsumerEventWait::Change, context)
            }
            None => self
                .shared
                .event_signal
                .arm_blocking(*registration)
                .map_err(translate_signal),
        };
        match armed {
            Ok(id) => {
                *registration = Some(id);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn arm_event_task(
        &self,
        registration: &mut Option<AssignedConsumerEventRegistration>,
        wait: AssignedConsumerEventWait,
        context: &Context<'_>,
    ) -> Result<AssignedConsumerEventRegistration, AssignedConsumerNextEventError> {
        let id = self
            .shared
            .event_signal
            .arm_task(*registration, wait, context.waker())
            .map_err(translate_signal)?;
        *registration = Some(id);
        Ok(id)
    }

    fn finish_next_event(&self, registration: &mut Option<AssignedConsumerEventRegistration>) {
        self.cancel_next_event(registration);
    }
}

const fn translate_signal(
    _error: AssignedConsumerEventSignalError,
) -> AssignedConsumerNextEventError {
    AssignedConsumerNextEventError::internal_invariant()
}
