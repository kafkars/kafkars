//! Exact pending-notification retry, failover, and off-reactor worker ownership.
mod backlog;
mod queue;
mod route;
mod route_retry;
mod shutdown;
mod worker;

pub(super) use super::PendingNotificationJob;
#[cfg(test)]
pub(super) use super::{
    PendingNotificationPermitPool, PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
};

pub(super) use backlog::PendingNotificationRecoveryDispatchOwner;
pub(crate) use backlog::{PendingNotificationBacklog, PendingNotificationRecovery};
#[cfg(test)]
pub(crate) use queue::{PendingRecoveryQueue, PendingRecoverySubmitErrorKind};
pub(crate) use route::PendingNotificationRoute;
pub(crate) use route::PendingNotificationRouteMode;
pub(crate) use route_retry::PendingNotificationRouteProgress;
#[cfg(test)]
pub(crate) use shutdown::PendingNotificationShutdownOwner;
pub(crate) use shutdown::{
    PendingNotificationCleanupOwner, PendingNotificationShutdownFailures,
    PendingPrimaryMissingError, PendingRecoveryStartupOwner,
};
#[cfg(test)]
pub(crate) use worker::{PendingRecoveryJoin, PendingRecoveryJoinOutcome};
pub(crate) use worker::{PendingRecoveryJoinError, PendingRecoveryWorker};

#[cfg(test)]
mod backlog_test;
#[cfg(test)]
mod queue_test;
#[cfg(test)]
mod route_retry_test;
#[cfg(test)]
mod route_test;
#[cfg(test)]
mod worker_test;
