//! Closed transactional Produce terminal facts and retained route evidence.

#[cfg(test)]
use std::sync::Arc;

use kafka_client_core::{
    DeliveryStatus, ProducerAttemptFailureKind, ProducerBatchSuccess, ProducerBrokerFailure,
    TransactionEpoch, TransactionSendAttempt, TransactionSendId,
};
use kafka_driver::RouteFailureToken;

use crate::protocol::produce_response::ProduceResponseProtocolFailure;

use super::route_refresh::ProduceRouteRefresh;

/// Stable reason one accepted transactional Produce did not succeed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionProduceFailureKind {
    Broker(ProducerBrokerFailure),
    Protocol(ProduceResponseProtocolFailure),
    Driver(ProducerAttemptFailureKind),
    CompletionLost,
    DriverShutdown,
}

/// Exact failure and authoritative driver delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionProduceFailure {
    kind: TransactionProduceFailureKind,
    delivery: DeliveryStatus,
}

impl TransactionProduceFailure {
    pub(super) const fn new(kind: TransactionProduceFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    pub(crate) const fn kind(self) -> TransactionProduceFailureKind {
        self.kind
    }

    pub(crate) const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }

    #[cfg(test)]
    pub(crate) const fn broker_code(self) -> Option<i16> {
        match self.kind {
            TransactionProduceFailureKind::Broker(failure) => Some(failure.code()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        kind: TransactionProduceFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self::new(kind, delivery)
    }
}

/// Deterministic lifecycle consequence of one accepted send terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionProduceTerminalFact {
    Succeeded {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        success: ProducerBatchSuccess,
    },
    AbortRequired {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        failure: TransactionProduceFailure,
    },
    Fatal {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        failure: TransactionProduceFailure,
    },
}

pub(super) enum RouteEvidence {
    Driver(Option<RouteFailureToken>),
    #[cfg(test)]
    Test(Arc<std::sync::atomic::AtomicBool>),
}

impl RouteEvidence {
    pub(super) const fn driver(token: Option<RouteFailureToken>) -> Self {
        Self::Driver(token)
    }

    pub(super) fn discard(self) {
        match self {
            Self::Driver(token) => drop(token),
            #[cfg(test)]
            Self::Test(discarded) => {
                discarded.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }
}

/// Correlated fact plus opaque partition-route evidence awaiting caller settlement.
#[must_use = "transactional Produce evidence must survive lifecycle settlement"]
pub(crate) struct TransactionProduceTerminal {
    #[cfg(test)]
    pub(super) topic: Arc<str>,
    #[cfg(test)]
    pub(super) partition: i32,
    attempt: TransactionSendAttempt,
    pub(super) fact: TransactionProduceTerminalFact,
    pub(super) evidence: RouteEvidence,
    pub(super) route_refresh: ProduceRouteRefresh,
}

impl TransactionProduceTerminal {
    pub(super) fn new(
        #[cfg(test)] topic: Arc<str>,
        #[cfg(test)] partition: i32,
        attempt: TransactionSendAttempt,
        fact: TransactionProduceTerminalFact,
        mut evidence: RouteEvidence,
    ) -> Self {
        let route_refresh = ProduceRouteRefresh::for_terminal(fact, &mut evidence);
        Self {
            #[cfg(test)]
            topic,
            #[cfg(test)]
            partition,
            attempt,
            fact,
            evidence,
            route_refresh,
        }
    }

    #[cfg(test)]
    pub(crate) const fn epoch(&self) -> TransactionEpoch {
        match self.fact {
            TransactionProduceTerminalFact::Succeeded { epoch, .. }
            | TransactionProduceTerminalFact::AbortRequired { epoch, .. }
            | TransactionProduceTerminalFact::Fatal { epoch, .. } => epoch,
        }
    }

    #[cfg(test)]
    pub(crate) const fn send_id(&self) -> TransactionSendId {
        match self.fact {
            TransactionProduceTerminalFact::Succeeded { send_id, .. }
            | TransactionProduceTerminalFact::AbortRequired { send_id, .. }
            | TransactionProduceTerminalFact::Fatal { send_id, .. } => send_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    #[cfg(test)]
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) const fn fact(&self) -> TransactionProduceTerminalFact {
        self.fact
    }

    pub(crate) const fn attempt(&self) -> TransactionSendAttempt {
        self.attempt
    }

    pub(crate) fn discard(self) {
        self.evidence.discard();
        drop(self.route_refresh);
    }
}
