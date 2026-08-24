//! Deadline-bounded invalidation of one failed broker-local share-fetch route.

use std::{mem, sync::Arc, time::Instant};

use kafka_client_core::{Deadline, Moment, partitioning::TopicMetadataGeneration};
use kafka_driver::{Call, InvalidationDisposition, RouteFailureToken, SubmitError};

use crate::driver::{DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicRouteViewCall};

use super::route::ShareFetchRoute;

/// Exact failed broker route retained until invalidation permits session replacement.
#[must_use = "a failed ShareFetch route must be invalidated or accepted"]
pub(crate) struct ShareFetchRouteRefresh {
    deadline: Deadline,
    state: ShareFetchRouteRefreshState,
}

enum ShareFetchRouteRefreshState {
    InvalidationQueued {
        token: RouteFailureToken,
        metadata: Option<ShareFetchMetadataRefresh>,
    },
    InvalidationActive {
        call: Call<InvalidationDisposition>,
        metadata: Option<ShareFetchMetadataRefresh>,
    },
    MetadataQueued(ShareFetchMetadataRefresh),
    MetadataActive(TopicRouteViewCall),
    Ready,
    Failed,
}

struct ShareFetchMetadataRefresh {
    topic: Arc<str>,
    observed: TopicMetadataGeneration,
    transport_deadline: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchRouteRefreshPoll {
    Progress,
    Pending,
    Ready,
    Failed,
}

impl ShareFetchRouteRefresh {
    pub(crate) fn try_new(
        route: ShareFetchRoute,
        deadline: Deadline,
    ) -> Result<Self, ShareFetchRoute> {
        route.into_broker_token().map(|token| Self {
            deadline,
            state: ShareFetchRouteRefreshState::InvalidationQueued {
                token,
                metadata: None,
            },
        })
    }

    pub(crate) fn try_new_with_metadata(
        route: ShareFetchRoute,
        deadline: Deadline,
        transport_deadline: Instant,
        topic: Arc<str>,
        observed: TopicMetadataGeneration,
    ) -> Result<Self, ShareFetchRoute> {
        route.into_broker_token().map(|token| Self {
            deadline,
            state: ShareFetchRouteRefreshState::InvalidationQueued {
                token,
                metadata: Some(ShareFetchMetadataRefresh {
                    topic,
                    observed,
                    transport_deadline,
                }),
            },
        })
    }

    pub(crate) fn poll(&mut self, driver: &DriverOwner, now: Moment) -> ShareFetchRouteRefreshPoll {
        if self.deadline.is_elapsed_at(now)
            && matches!(
                self.state,
                ShareFetchRouteRefreshState::InvalidationQueued { .. }
                    | ShareFetchRouteRefreshState::MetadataQueued(_)
            )
        {
            self.state = ShareFetchRouteRefreshState::Failed;
            return ShareFetchRouteRefreshPoll::Failed;
        }
        match mem::replace(&mut self.state, ShareFetchRouteRefreshState::Failed) {
            ShareFetchRouteRefreshState::InvalidationQueued { token, metadata } => {
                match driver.driver.invalidate(token) {
                    Ok(call) => {
                        self.state =
                            ShareFetchRouteRefreshState::InvalidationActive { call, metadata };
                        ShareFetchRouteRefreshPoll::Progress
                    }
                    Err(rejection) => {
                        let retryable = matches!(rejection.reason(), SubmitError::Full);
                        let (_source, token) = rejection.into_parts();
                        if retryable {
                            self.state =
                                ShareFetchRouteRefreshState::InvalidationQueued { token, metadata };
                            ShareFetchRouteRefreshPoll::Pending
                        } else {
                            drop(token);
                            ShareFetchRouteRefreshPoll::Failed
                        }
                    }
                }
            }
            ShareFetchRouteRefreshState::InvalidationActive { call, metadata } => {
                match call.try_result() {
                    None => {
                        self.state =
                            ShareFetchRouteRefreshState::InvalidationActive { call, metadata };
                        ShareFetchRouteRefreshPoll::Pending
                    }
                    Some(Ok(
                        InvalidationDisposition::Applied | InvalidationDisposition::IgnoredStale,
                    )) => {
                        if let Some(metadata) = metadata {
                            self.state = ShareFetchRouteRefreshState::MetadataQueued(metadata);
                            ShareFetchRouteRefreshPoll::Progress
                        } else {
                            self.state = ShareFetchRouteRefreshState::Ready;
                            ShareFetchRouteRefreshPoll::Ready
                        }
                    }
                    Some(Ok(_) | Err(_)) => ShareFetchRouteRefreshPoll::Failed,
                }
            }
            ShareFetchRouteRefreshState::MetadataQueued(metadata) => {
                match TopicRouteViewCall::submit_newer_than(
                    driver,
                    &metadata.topic,
                    metadata.observed,
                    metadata.transport_deadline,
                ) {
                    Ok(call) => {
                        self.state = ShareFetchRouteRefreshState::MetadataActive(call);
                        ShareFetchRouteRefreshPoll::Progress
                    }
                    Err(error) if error.kind() == TopicPartitionCountAdmissionFailureKind::Full => {
                        self.state = ShareFetchRouteRefreshState::MetadataQueued(metadata);
                        ShareFetchRouteRefreshPoll::Pending
                    }
                    Err(_error) => ShareFetchRouteRefreshPoll::Failed,
                }
            }
            ShareFetchRouteRefreshState::MetadataActive(mut call) => match call.try_terminal() {
                None => {
                    self.state = ShareFetchRouteRefreshState::MetadataActive(call);
                    ShareFetchRouteRefreshPoll::Pending
                }
                Some(Ok(_view)) => {
                    self.state = ShareFetchRouteRefreshState::Ready;
                    ShareFetchRouteRefreshPoll::Ready
                }
                Some(Err(_error)) => ShareFetchRouteRefreshPoll::Failed,
            },
            ShareFetchRouteRefreshState::Ready => {
                self.state = ShareFetchRouteRefreshState::Ready;
                ShareFetchRouteRefreshPoll::Ready
            }
            ShareFetchRouteRefreshState::Failed => ShareFetchRouteRefreshPoll::Failed,
        }
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        if let ShareFetchRouteRefreshState::MetadataActive(call) =
            mem::replace(&mut self.state, ShareFetchRouteRefreshState::Ready)
        {
            call.discard_after_driver_shutdown();
        }
        self.state = ShareFetchRouteRefreshState::Ready;
    }
}
