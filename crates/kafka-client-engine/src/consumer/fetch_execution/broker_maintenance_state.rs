//! Linear prepared, active, and faulted forgotten-only session ownership.

use crate::driver::{
    ForgottenFetchCompletionFailure, ForgottenFetchConfirmation, ForgottenFetchRequest,
    TrackedForgottenFetchCall,
};

use super::{broker_session::BrokerSessionPlan, executor::DirectFetchExecutor};

pub(super) enum BrokerSessionMaintenance {
    Prepared {
        plan: BrokerSessionPlan,
        request: ForgottenFetchRequest,
    },
    Active {
        plan: BrokerSessionPlan,
        call: TrackedForgottenFetchCall,
    },
    CompletionFault {
        _plan: BrokerSessionPlan,
        failure: ForgottenFetchCompletionFailure,
    },
    ConfirmationFault {
        request: ForgottenFetchRequest,
        confirmation: ForgottenFetchConfirmation,
    },
    RequestFault {
        request: ForgottenFetchRequest,
    },
}

impl DirectFetchExecutor {
    pub(crate) fn broker_session_maintenance_deadline(
        &self,
    ) -> Option<kafka_client_core::Deadline> {
        match self.broker_maintenance.as_ref() {
            Some(BrokerSessionMaintenance::Prepared { request, .. }) => {
                Some(request.deadline().core())
            }
            _ => None,
        }
    }

    pub(super) fn release_forgotten_maintenance_after_driver_shutdown(&mut self) {
        let Some(maintenance) = self.broker_maintenance.take() else {
            return;
        };
        match maintenance {
            BrokerSessionMaintenance::Prepared { plan, request } => drop((plan, request)),
            BrokerSessionMaintenance::Active { plan, call } => {
                let request = call.recover_after_driver_shutdown();
                drop((plan, request));
            }
            BrokerSessionMaintenance::CompletionFault {
                _plan: plan,
                failure,
            } => {
                let recovered = failure.recover_after_driver_shutdown();
                drop((plan, recovered));
            }
            BrokerSessionMaintenance::ConfirmationFault {
                request,
                confirmation,
            } => {
                confirmation.discard_after_driver_shutdown();
                drop(request);
            }
            BrokerSessionMaintenance::RequestFault { request } => drop(request),
        }
    }
}
