//! Explicitly bounded primary-notifier retry for retained pending jobs.

use std::num::NonZeroUsize;

use crate::completion::{CompletionRegistry, CompletionRegistryError};

use super::{
    PendingNotificationBacklog,
    route::{
        PendingNotificationRoute, PendingNotificationRouteMode, PendingNotificationRouteState,
        take_backlog,
    },
};

/// Bounded retry facts without any retained notification owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingNotificationRouteProgress {
    attempted: usize,
    remaining: bool,
    blocked: bool,
    mode: PendingNotificationRouteMode,
}

impl PendingNotificationRouteProgress {
    const fn new(
        attempted: usize,
        remaining: bool,
        blocked: bool,
        mode: PendingNotificationRouteMode,
    ) -> Self {
        Self {
            attempted,
            remaining,
            blocked,
            mode,
        }
    }

    pub(crate) const fn attempted(self) -> usize {
        self.attempted
    }

    pub(crate) const fn remaining(self) -> bool {
        self.remaining
    }

    pub(crate) const fn blocked(self) -> bool {
        self.blocked
    }

    pub(crate) const fn mode(self) -> PendingNotificationRouteMode {
        self.mode
    }

    #[cfg(test)]
    pub(crate) const fn primary_for_test(attempted: usize, remaining: bool, blocked: bool) -> Self {
        Self::new(
            attempted,
            remaining,
            blocked,
            PendingNotificationRouteMode::Primary,
        )
    }
}

impl PendingNotificationRoute {
    /// Attempts no more than `limit` submissions to the older primary notifier.
    pub(crate) fn retry_primary_notifications<T: Send + 'static>(
        &mut self,
        primary: &CompletionRegistry<T>,
        limit: NonZeroUsize,
    ) -> PendingNotificationRouteProgress {
        let PendingNotificationRouteState::Primary(backlog) = &mut self.state else {
            let remaining = self.retained_len() != 0;
            return PendingNotificationRouteProgress::new(
                0,
                remaining,
                remaining,
                PendingNotificationRouteMode::Recovery,
            );
        };
        let progress = retry_primary(backlog, primary, limit.get());
        if progress.stopped {
            let retained = take_backlog(backlog).into_recovery_all();
            self.state = PendingNotificationRouteState::Recovery { retained };
        }
        let remaining = self.retained_len() != 0;
        PendingNotificationRouteProgress::new(
            progress.attempted,
            remaining,
            progress.blocked && remaining,
            self.mode(),
        )
    }
}

struct PrimaryRetryProgress {
    attempted: usize,
    blocked: bool,
    stopped: bool,
}

fn retry_primary<T: Send + 'static>(
    backlog: &mut PendingNotificationBacklog,
    primary: &CompletionRegistry<T>,
    limit: usize,
) -> PrimaryRetryProgress {
    let mut attempted = 0;
    while attempted < limit {
        let Some(job) = backlog.pop_front() else {
            break;
        };
        attempted += 1;
        match primary.notify_pending(job) {
            Ok(()) => {}
            Err((CompletionRegistryError::NotificationBackpressure, returned)) => {
                backlog.push_front(returned);
                return PrimaryRetryProgress {
                    attempted,
                    blocked: true,
                    stopped: false,
                };
            }
            Err((_stopped, returned)) => {
                backlog.push_front(returned);
                return PrimaryRetryProgress {
                    attempted,
                    blocked: true,
                    stopped: true,
                };
            }
        }
    }
    PrimaryRetryProgress {
        attempted,
        blocked: false,
        stopped: false,
    }
}
