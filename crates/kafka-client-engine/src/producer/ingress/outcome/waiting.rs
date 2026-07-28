//! Classification of bounded pre-admission waiting ownership.

use crate::producer::{
    ProducerRejectionReason,
    waiting::{AdmittedWaiting, ProducerWaitingAdmissionFailure},
};

use super::{
    ProducerPortAccepted, ProducerPortAdmissionError, ProducerPortPoison, ProducerPortPoisonReason,
    ProducerPortRejectionReason, poisoned_before, rejected,
};

#[allow(
    clippy::result_large_err,
    reason = "ownership-preserving rejection returns the intact record"
)]
pub(in crate::producer::ingress) fn classify_waiting_admission(
    result: Result<AdmittedWaiting, ProducerWaitingAdmissionFailure>,
) -> Result<ProducerPortAccepted, ProducerPortAdmissionError> {
    match result {
        Ok(admitted) => {
            let (waiter_id, observer, token, fault) = admitted.into_port_parts();
            Ok(ProducerPortAccepted {
                observer,
                operation_id: None,
                waiting: Some((waiter_id, token)),
                fault: fault.map_or(Ok(()), |error| {
                    Err(super::ProducerPortAcceptedFault::HostInvariant(error))
                }),
            })
        }
        Err(ProducerWaitingAdmissionFailure::Rejected(rejection)) => {
            let reason = rejection.reason;
            if let ProducerRejectionReason::HostPoisoned(error) = reason {
                return Err(poisoned_before(
                    rejection.record,
                    ProducerPortPoisonReason::Host(error),
                ));
            }
            Err(rejected(
                rejection.record,
                ProducerPortRejectionReason::Host(reason),
            ))
        }
        Err(ProducerWaitingAdmissionFailure::Invariant { error, record }) => {
            Err(ProducerPortAdmissionError::Poisoned(
                ProducerPortPoison::BeforeOwnership { error, record },
            ))
        }
    }
}
