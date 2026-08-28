//! Borrow-before-route ownership of one immutable topic view and cohort key.

mod admission;
mod resolution;
#[cfg(test)]
mod resolution_test;

use kafka_client_core::ProducerAttemptFailureKind;

use crate::{
    driver::{
        DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicPartitionCountFailure,
        TopicRouteView, TopicRouteViewCall,
    },
    producer::execution::PreparedProduceRouteKey,
};

pub(in crate::engine_host) use admission::{admit, apply_ready, discard_after_driver_shutdown};

/// One route key retained without taking any materialized Produce owner.
pub(in crate::engine_host) struct ProducerRoutingCall {
    state: ProducerRoutingState,
}

enum ProducerRoutingState {
    Queued {
        key: PreparedProduceRouteKey,
    },
    Active {
        key: PreparedProduceRouteKey,
        call: TopicRouteViewCall,
    },
    Ready {
        key: PreparedProduceRouteKey,
        view: TopicRouteView,
    },
    Failed {
        key: PreparedProduceRouteKey,
        failure: RoutingFailure,
    },
    Empty,
}

enum RoutingStartPoll {
    Submitted,
    Pending,
    Failed,
}

#[derive(Clone, Copy)]
enum RoutingFailure {
    Deadline,
    Attempt(ProducerAttemptFailureKind),
}

impl ProducerRoutingCall {
    pub(in crate::engine_host) const fn new(key: PreparedProduceRouteKey) -> Self {
        Self {
            state: ProducerRoutingState::Queued { key },
        }
    }

    pub(in crate::engine_host) fn deadline(&self) -> Option<crate::clock::OperationDeadline> {
        self.key().map(PreparedProduceRouteKey::deadline)
    }

    fn key(&self) -> Option<&PreparedProduceRouteKey> {
        match &self.state {
            ProducerRoutingState::Queued { key }
            | ProducerRoutingState::Active { key, .. }
            | ProducerRoutingState::Ready { key, .. }
            | ProducerRoutingState::Failed { key, .. } => Some(key),
            ProducerRoutingState::Empty => None,
        }
    }

    fn try_start(&mut self, driver: &DriverOwner) -> RoutingStartPoll {
        let ProducerRoutingState::Queued { key } = &self.state else {
            return RoutingStartPoll::Pending;
        };
        let result = match key.retry_topic_identity() {
            Some((_expected, generation)) => TopicRouteViewCall::submit_newer_than(
                driver,
                key.topic(),
                generation,
                key.deadline().transport(),
            ),
            None => TopicRouteViewCall::submit(driver, key.topic(), key.deadline().transport()),
        };
        match result {
            Ok(call) => {
                let ProducerRoutingState::Queued { key } =
                    std::mem::replace(&mut self.state, ProducerRoutingState::Empty)
                else {
                    unreachable!("queued Produce route key cannot change during admission")
                };
                self.state = ProducerRoutingState::Active { key, call };
                RoutingStartPoll::Submitted
            }
            Err(error) if error.kind() == TopicPartitionCountAdmissionFailureKind::Full => {
                RoutingStartPoll::Pending
            }
            Err(_error) => {
                self.fail_queued(RoutingFailure::Attempt(
                    ProducerAttemptFailureKind::RouteUnavailable,
                ));
                RoutingStartPoll::Failed
            }
        }
    }

    fn poll(&mut self) -> bool {
        let ProducerRoutingState::Active { call, .. } = &mut self.state else {
            return false;
        };
        let Some(result) = call.try_terminal() else {
            return false;
        };
        let ProducerRoutingState::Active {
            key,
            call: _completed,
        } = std::mem::replace(&mut self.state, ProducerRoutingState::Empty)
        else {
            unreachable!("terminal Produce topic view retains its exact key")
        };
        self.state = match result {
            Ok(view) => ProducerRoutingState::Ready { key, view },
            Err(TopicPartitionCountFailure::Deadline) => ProducerRoutingState::Failed {
                key,
                failure: RoutingFailure::Deadline,
            },
            Err(_failure) => ProducerRoutingState::Failed {
                key,
                failure: RoutingFailure::Attempt(ProducerAttemptFailureKind::RouteUnavailable),
            },
        };
        true
    }

    fn ready(&self) -> Option<(&PreparedProduceRouteKey, &TopicRouteView)> {
        match &self.state {
            ProducerRoutingState::Ready { key, view } => Some((key, view)),
            _ => None,
        }
    }

    fn failure(&self) -> Option<RoutingFailure> {
        match self.state {
            ProducerRoutingState::Failed { failure, .. } => Some(failure),
            _ => None,
        }
    }

    fn abandon(&mut self) {
        let state = std::mem::replace(&mut self.state, ProducerRoutingState::Empty);
        if let ProducerRoutingState::Active { call, .. } = state {
            call.abandon();
        }
    }

    fn fail_queued(&mut self, failure: RoutingFailure) {
        let ProducerRoutingState::Queued { key } =
            std::mem::replace(&mut self.state, ProducerRoutingState::Empty)
        else {
            return;
        };
        self.state = ProducerRoutingState::Failed { key, failure };
    }

    fn discard_after_driver_shutdown(self) {
        if let ProducerRoutingState::Active { call, .. } = self.state {
            call.discard_after_driver_shutdown();
        }
    }
}
