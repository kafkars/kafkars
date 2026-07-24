//! Atomic delivery probe and wait registration under the assigned-owner lock.

use std::task::{Context, Poll};

use crate::consumer::{
    assigned_host::{
        AssignedConsumerDelivery, AssignedConsumerPort, AssignedConsumerShardLockError,
    },
    assigned_owner_model::AssignedConsumerOwnerError,
};

use super::{
    AssignedConsumerRecvError, AssignedConsumerRecvRegistration,
    signal::{AssignedConsumerRecvSignalError, AssignedConsumerRecvWait},
};

impl AssignedConsumerPort {
    pub(super) fn poll_recv(
        &self,
        registration: &mut Option<AssignedConsumerRecvRegistration>,
        context: &Context<'_>,
    ) -> Poll<Result<Option<AssignedConsumerDelivery>, AssignedConsumerRecvError>> {
        for attempt in 0..2 {
            match self.shared.try_owner() {
                Ok(mut guard) => {
                    let result = self.probe_recv(&mut guard, registration, Some(context));
                    drop(guard);
                    self.shared
                        .request_recv_notification(AssignedConsumerRecvWait::Unlock);
                    return result;
                }
                Err(AssignedConsumerShardLockError::Contended) => {
                    if let Err(error) =
                        self.arm_task(registration, AssignedConsumerRecvWait::Unlock, context)
                    {
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
                    self.finish_recv(registration);
                    return Poll::Ready(Err(AssignedConsumerRecvError::host_unavailable()));
                }
            }
        }
        Poll::Pending
    }

    pub(super) fn wait_recv(
        &self,
        registration: &mut Option<AssignedConsumerRecvRegistration>,
    ) -> Result<Option<AssignedConsumerDelivery>, AssignedConsumerRecvError> {
        loop {
            let mut guard = self
                .shared
                .owner()
                .map_err(|_error| AssignedConsumerRecvError::host_unavailable())?;
            match self.probe_recv(&mut guard, registration, None) {
                Poll::Ready(result) => {
                    drop(guard);
                    self.shared
                        .request_recv_notification(AssignedConsumerRecvWait::Unlock);
                    return result;
                }
                Poll::Pending => {
                    let Some(id) = *registration else {
                        return Err(AssignedConsumerRecvError::internal_invariant());
                    };
                    drop(guard);
                    self.shared
                        .request_recv_notification(AssignedConsumerRecvWait::Unlock);
                    self.shared.recv_signal.wait(id).map_err(translate_signal)?;
                }
            }
        }
    }

    pub(super) fn cancel_recv(&self, registration: &mut Option<AssignedConsumerRecvRegistration>) {
        if let Some(id) = registration.take() {
            self.shared.recv_signal.cancel(id);
        }
    }

    fn probe_recv(
        &self,
        guard: &mut Option<crate::consumer::AssignedConsumerOwner>,
        registration: &mut Option<AssignedConsumerRecvRegistration>,
        context: Option<&Context<'_>>,
    ) -> Poll<Result<Option<AssignedConsumerDelivery>, AssignedConsumerRecvError>> {
        if self.shared.assigned_admission_is_closed() {
            self.finish_recv(registration);
            return Poll::Ready(Ok(None));
        }
        let Some(owner) = guard.as_mut() else {
            self.finish_recv(registration);
            return Poll::Ready(Err(AssignedConsumerRecvError::host_unavailable()));
        };
        match owner.take_named_delivery() {
            Ok(Some(delivery)) => {
                self.finish_recv(registration);
                Poll::Ready(Ok(Some(delivery)))
            }
            Ok(None) | Err(AssignedConsumerOwnerError::DeliveryUnavailable) => {
                let armed = match context {
                    Some(context) => {
                        self.arm_task(registration, AssignedConsumerRecvWait::Change, context)
                    }
                    None => self
                        .shared
                        .recv_signal
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
            Err(AssignedConsumerOwnerError::Faulted) => {
                self.finish_recv(registration);
                Poll::Ready(Err(AssignedConsumerRecvError::host_unavailable()))
            }
            Err(_) => {
                self.finish_recv(registration);
                Poll::Ready(Err(AssignedConsumerRecvError::internal_invariant()))
            }
        }
    }

    fn arm_task(
        &self,
        registration: &mut Option<AssignedConsumerRecvRegistration>,
        wait: AssignedConsumerRecvWait,
        context: &Context<'_>,
    ) -> Result<AssignedConsumerRecvRegistration, AssignedConsumerRecvError> {
        let id = self
            .shared
            .recv_signal
            .arm_task(*registration, wait, context.waker())
            .map_err(translate_signal)?;
        *registration = Some(id);
        Ok(id)
    }

    fn finish_recv(&self, registration: &mut Option<AssignedConsumerRecvRegistration>) {
        self.cancel_recv(registration);
    }
}

const fn translate_signal(_error: AssignedConsumerRecvSignalError) -> AssignedConsumerRecvError {
    AssignedConsumerRecvError::internal_invariant()
}
