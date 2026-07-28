//! Private runtime-neutral translation over classic-group event observation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    GroupConsumerNextEvent as EngineNextEvent, GroupConsumerNextEventError,
    GroupConsumerNextEventErrorKind, GroupConsumerRevocationControl,
};

use super::group_consumer_rebalance_event::translate_group_consumer_event;
use crate::{ErrorKind, KafkaError, consumer::ConsumerEvent};

pub(crate) struct GroupConsumerNextEvent<'consumer> {
    inner: GroupConsumerNextEventInner<'consumer>,
}

enum GroupConsumerNextEventInner<'consumer> {
    Engine {
        inner: EngineNextEvent<'consumer>,
        revocation: Option<GroupConsumerRevocationControl>,
    },
    Rejected(Option<KafkaError>),
}

impl<'consumer> GroupConsumerNextEvent<'consumer> {
    pub(super) const fn from_engine(
        inner: EngineNextEvent<'consumer>,
        revocation: GroupConsumerRevocationControl,
    ) -> Self {
        Self {
            inner: GroupConsumerNextEventInner::Engine {
                inner,
                revocation: Some(revocation),
            },
        }
    }

    pub(super) fn rejected(error: KafkaError) -> Self {
        Self {
            inner: GroupConsumerNextEventInner::Rejected(Some(error)),
        }
    }

    pub(crate) fn wait(self) -> Result<Option<ConsumerEvent>, KafkaError> {
        match self.inner {
            GroupConsumerNextEventInner::Engine {
                inner,
                mut revocation,
            } => inner
                .wait()
                .map(|event| {
                    event.map(|event| {
                        translate_group_consumer_event(
                            event,
                            revocation.take().unwrap_or_else(missing_revocation_control),
                        )
                    })
                })
                .map_err(translate_error),
            GroupConsumerNextEventInner::Rejected(mut error) => {
                Err(error.take().unwrap_or_else(observed_twice))
            }
        }
    }
}

impl Future for GroupConsumerNextEvent<'_> {
    type Output = Result<Option<ConsumerEvent>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            GroupConsumerNextEventInner::Engine { inner, revocation } => {
                match Pin::new(inner).poll(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => Poll::Ready(
                        result
                            .map(|event| {
                                event.map(|event| {
                                    translate_group_consumer_event(
                                        event,
                                        revocation
                                            .take()
                                            .unwrap_or_else(missing_revocation_control),
                                    )
                                })
                            })
                            .map_err(translate_error),
                    ),
                }
            }
            GroupConsumerNextEventInner::Rejected(error) => {
                Poll::Ready(Err(error.take().unwrap_or_else(observed_twice)))
            }
        }
    }
}

impl std::fmt::Debug for GroupConsumerNextEvent<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerNextEvent")
            .finish_non_exhaustive()
    }
}

fn translate_error(error: GroupConsumerNextEventError) -> KafkaError {
    let message = match error.kind() {
        GroupConsumerNextEventErrorKind::HostUnavailable => "group event host is unavailable",
        GroupConsumerNextEventErrorKind::InternalInvariant => {
            "group event observation is inconsistent"
        }
    };
    KafkaError::new(ErrorKind::Internal, message)
}

fn observed_twice() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "group event startup error was already observed",
    )
}

fn missing_revocation_control() -> GroupConsumerRevocationControl {
    unreachable!("one event observation owns one revocation completion capability")
}
