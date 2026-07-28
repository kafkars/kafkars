//! Linear tracked submission and completion ownership for transactional Produce.

use std::{error::Error, fmt, sync::Arc};

use kafka_client_core::{
    DeliveryStatus, Moment, ProducerAttemptFailureKind, TransactionEpoch, TransactionSendAttempt,
    TransactionSendId,
};
use kafka_driver::RoutedCall;
use kafka_wire::ProduceResponse;

use crate::{
    clock::OperationDeadline,
    driver::{DriverOwner, rpc::ProduceSubmitError},
    protocol::produce::MaterializedProduce,
};

use super::{
    model::{RouteEvidence, TransactionProduceTerminal},
    normalize::{TransactionProduceResult, normalize_terminal},
};

/// One driver-accepted call with exact transaction and partition correlation.
#[must_use = "an accepted transactional Produce requires terminal settlement"]
pub(crate) struct TransactionProduceCall {
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    attempt: TransactionSendAttempt,
    topic: Arc<str>,
    partition: i32,
    call: Option<RoutedCall<ProduceResponse>>,
}

impl TransactionProduceCall {
    #[expect(
        clippy::too_many_arguments,
        reason = "exact transaction, send, route, time, and byte owners cross one admission boundary"
    )]
    pub(crate) fn submit(
        driver: &DriverOwner,
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        attempt: TransactionSendAttempt,
        transactional_id: &str,
        materialized: &MaterializedProduce,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<Self, TransactionProduceCallAdmissionFailure> {
        let topic = materialized.topic_owner();
        let partition = materialized.partition();
        let request =
            materialized.transactional_name_routed_request(transactional_id, now, deadline);
        let call = driver
            .submit_tracked_produce(topic.as_ref(), partition, request, deadline.transport())
            .map_err(|source| TransactionProduceCallAdmissionFailure {
                #[cfg(test)]
                epoch,
                #[cfg(test)]
                send_id,
                source,
            })?;
        Ok(Self {
            epoch,
            send_id,
            attempt,
            topic,
            partition,
            call: Some(call),
        })
    }

    #[cfg(test)]
    pub(crate) const fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    #[cfg(test)]
    pub(crate) const fn send_id(&self) -> TransactionSendId {
        self.send_id
    }

    #[cfg(test)]
    pub(crate) const fn attempt(&self) -> TransactionSendAttempt {
        self.attempt
    }

    #[cfg(test)]
    pub(crate) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    #[cfg(test)]
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) fn try_terminal(&mut self) -> Option<TransactionProduceTerminal> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        let (result, evidence) = match result {
            Ok(outcome) => {
                let (result, _selected_version, token) = outcome.into_parts();
                let result = match result {
                    Ok(response) => TransactionProduceResult::Response(response),
                    Err(error) => TransactionProduceResult::Driver(error),
                };
                (result, RouteEvidence::driver(token))
            }
            Err(_completion_error) => (
                TransactionProduceResult::CompletionLost,
                RouteEvidence::driver(None),
            ),
        };
        Some(self.terminal(result, evidence))
    }

    pub(crate) fn recover_after_driver_shutdown(mut self) -> TransactionProduceTerminal {
        drop(self.call.take());
        self.terminal(
            TransactionProduceResult::DriverShutdown,
            RouteEvidence::driver(None),
        )
    }

    fn terminal(
        &self,
        result: TransactionProduceResult,
        evidence: RouteEvidence,
    ) -> TransactionProduceTerminal {
        normalize_terminal(
            self.epoch,
            self.send_id,
            Arc::clone(&self.topic),
            self.partition,
            self.attempt,
            result,
            evidence,
        )
    }
}

/// Explicit definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
pub(crate) struct TransactionProduceCallAdmissionFailure {
    #[cfg(test)]
    epoch: TransactionEpoch,
    #[cfg(test)]
    send_id: TransactionSendId,
    source: ProduceSubmitError,
}

impl TransactionProduceCallAdmissionFailure {
    #[cfg(test)]
    pub(crate) const fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    #[cfg(test)]
    pub(crate) const fn send_id(&self) -> TransactionSendId {
        self.send_id
    }

    #[expect(
        clippy::unused_self,
        reason = "delivery certainty is evidence carried by this exact admission failure"
    )]
    pub(crate) const fn delivery(&self) -> DeliveryStatus {
        DeliveryStatus::NotSent
    }

    pub(crate) const fn failure_kind(&self) -> ProducerAttemptFailureKind {
        self.source.failure_kind()
    }
}

impl fmt::Display for TransactionProduceCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transactional Produce admission: {}",
            self.source
        )
    }
}

impl Error for TransactionProduceCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
