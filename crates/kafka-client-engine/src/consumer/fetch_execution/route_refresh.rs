//! Causal metadata refresh retained beside each KIP-951 replacement Fetch.

use crate::driver::{BrokerId, DriverOwner, FetchRouteRefresh, FetchRouteRefreshPoll};

use super::{executor::DirectFetchExecutor, prepared::PreparedFetchExecution};

pub(super) struct LeaderMovementRecovery {
    attempts: Vec<LeaderMovementAttempt>,
}

struct LeaderMovementAttempt {
    refresh: Option<FetchRouteRefresh>,
    waiting: Option<PreparedFetchExecution>,
    hinted_broker: Option<BrokerId>,
    failed: bool,
}

pub(super) enum WaitingLeaderRoute {
    Ready {
        prepared: PreparedFetchExecution,
        hinted_broker: Option<BrokerId>,
    },
    Failed {
        prepared: PreparedFetchExecution,
        hinted_broker: Option<BrokerId>,
    },
}

impl LeaderMovementRecovery {
    pub(super) const fn new() -> Self {
        Self {
            attempts: Vec::new(),
        }
    }

    pub(super) fn try_reserve(&mut self, capacity: usize) -> Result<(), ()> {
        self.attempts
            .try_reserve_exact(capacity)
            .map_err(|_error| ())
    }

    pub(super) fn can_retain_attempt(&self) -> bool {
        self.attempts.len() < self.attempts.capacity()
    }

    pub(super) fn begin(
        &mut self,
        refresh: Option<FetchRouteRefresh>,
        waiting: Option<PreparedFetchExecution>,
        hinted_broker: Option<BrokerId>,
    ) {
        if refresh.is_some() || waiting.is_some() {
            self.attempts.push(LeaderMovementAttempt {
                refresh,
                waiting,
                hinted_broker,
                failed: false,
            });
        }
    }

    pub(super) fn poll(&mut self, driver: &DriverOwner) -> bool {
        let Some(index) = self
            .attempts
            .iter()
            .position(|attempt| attempt.refresh.is_some())
        else {
            return false;
        };
        let result = self.attempts[index].refresh.as_mut().map_or_else(
            || unreachable!("selected attempt has a refresh"),
            |refresh| refresh.poll(driver),
        );
        match result {
            FetchRouteRefreshPoll::Pending => return false,
            FetchRouteRefreshPoll::Ready => self.attempts[index].refresh = None,
            FetchRouteRefreshPoll::Failed => {
                self.attempts[index].refresh = None;
                self.attempts[index].failed = true;
            }
        }
        if self.attempts[index].waiting.is_none() {
            self.attempts.swap_remove(index);
        }
        true
    }

    pub(super) fn take_waiting(&mut self) -> Option<WaitingLeaderRoute> {
        let index = self
            .attempts
            .iter()
            .position(|attempt| attempt.refresh.is_none() && attempt.waiting.is_some())?;
        let mut attempt = self.attempts.swap_remove(index);
        let waiting = attempt
            .waiting
            .take()
            .unwrap_or_else(|| unreachable!("selected attempt has a waiter"));
        Some(if attempt.failed {
            WaitingLeaderRoute::Failed {
                prepared: waiting,
                hinted_broker: attempt.hinted_broker,
            }
        } else {
            WaitingLeaderRoute::Ready {
                prepared: waiting,
                hinted_broker: attempt.hinted_broker,
            }
        })
    }

    pub(super) fn restore_waiting(&mut self, waiting: WaitingLeaderRoute) {
        let (waiting, hinted_broker, failed) = match waiting {
            WaitingLeaderRoute::Ready {
                prepared,
                hinted_broker,
            } => (prepared, hinted_broker, false),
            WaitingLeaderRoute::Failed {
                prepared,
                hinted_broker,
            } => (prepared, hinted_broker, true),
        };
        self.attempts.push(LeaderMovementAttempt {
            refresh: None,
            waiting: Some(waiting),
            hinted_broker,
            failed,
        });
    }

    pub(super) fn retained(&self) -> usize {
        self.attempts.iter().fold(0usize, |retained, attempt| {
            retained
                .saturating_add(usize::from(attempt.refresh.is_some()))
                .saturating_add(usize::from(attempt.waiting.is_some()))
        })
    }

    pub(super) fn recover_after_driver_shutdown(self) -> Vec<PreparedFetchExecution> {
        self.attempts
            .into_iter()
            .filter_map(|attempt| attempt.waiting)
            .collect()
    }
}

impl DirectFetchExecutor {
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission capacity rejection returns the exact linear prepared Fetch owner"
    )]
    pub(super) fn retain_topic_route_retry(
        &mut self,
        prepared: PreparedFetchExecution,
    ) -> Result<(), PreparedFetchExecution> {
        if !self.leader_recovery.can_retain_attempt() {
            return Err(prepared);
        }
        self.leader_recovery.begin(None, Some(prepared), None);
        Ok(())
    }
}
