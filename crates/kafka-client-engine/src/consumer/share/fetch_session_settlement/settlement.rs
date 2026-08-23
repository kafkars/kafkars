//! Atomic `ShareFetch` terminal interpretation and decoded delivery staging.

use core::num::NonZeroI16;

use kafka_client_core::{DeliveryStatus, Moment, ShareFetchSettlementErrorKind};

use crate::{
    driver::{
        ShareFetchFailureKind, ShareFetchResolution, ShareFetchRoute, ShareFetchTerminalContext,
    },
    protocol::consumer::share_fetch::ShareFetchEndpoint,
};

use super::super::{
    fetch_acquisition_decode::{ShareFetchAcquisitionDecodeError, decode_share_fetch_success},
    fetch_delivery::ShareFetchDeliveryPartition,
    fetch_session::{ShareFetchSessionOwner, ShareFetchSessionOwnerError},
};

/// Decoded records and route receipt retained after atomic core admission.
#[must_use = "staged share delivery must be exposed or released"]
pub(in crate::consumer::share) struct StagedShareFetchDelivery {
    pub(in crate::consumer::share) fence: kafka_client_core::ShareFetchSessionFence,
    pub(in crate::consumer::share) route: ShareFetchRoute,
    pub(in crate::consumer::share) throttle_time_ms: u32,
    pub(in crate::consumer::share) endpoints: Vec<ShareFetchEndpoint>,
    pub(in crate::consumer::share) partitions: Vec<ShareFetchDeliveryPartition>,
    pub(in crate::consumer::share) acquisitions: usize,
}

impl ShareFetchSessionOwner {
    pub(in crate::consumer::share) fn settle_terminal(
        &mut self,
        now: Moment,
    ) -> Result<ShareFetchSettlementTurn, ShareFetchTerminalSettlementError> {
        if self.staged.is_some() {
            return Err(ShareFetchTerminalSettlementError::Occupied);
        }
        let terminal = self
            .take_terminal()
            .ok_or(ShareFetchTerminalSettlementError::MissingTerminal)?;
        let attempt = terminal.attempt;
        match terminal.resolution {
            ShareFetchResolution::Succeeded(success) => {
                let Some(timeout_ms) = success
                    .acquisition_lock_timeout_ms
                    .or(self.lock_timeout_ms())
                else {
                    terminal.route.accept();
                    self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                        .map_err(ShareFetchTerminalSettlementError::Session)?;
                    return Err(ShareFetchTerminalSettlementError::MissingLockTimeout);
                };
                let lock_deadline = match lock_deadline(terminal.context, timeout_ms) {
                    Ok(deadline) => deadline,
                    Err(error) => {
                        terminal.route.accept();
                        self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                            .map_err(ShareFetchTerminalSettlementError::Session)?;
                        return Err(error);
                    }
                };
                let throttle_until = match throttle_deadline(now, success.throttle_time_ms) {
                    Ok(deadline) => deadline,
                    Err(error) => {
                        terminal.route.accept();
                        self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                            .map_err(ShareFetchTerminalSettlementError::Session)?;
                        return Err(error);
                    }
                };
                let response_timeout = success.acquisition_lock_timeout_ms;
                let decoded = decode_share_fetch_success(
                    success,
                    self.request_plan(),
                    lock_deadline,
                    now,
                    self.decode_limits(),
                )
                .map_err(ShareFetchTerminalSettlementError::Decode);
                let decoded = match decoded {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        terminal.route.accept();
                        self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                            .map_err(ShareFetchTerminalSettlementError::Session)?;
                        return Err(error);
                    }
                };
                let acquisitions = match self.settle_acquired(attempt, now, decoded.ranges) {
                    Ok(acquisitions) => acquisitions,
                    Err(error) => {
                        terminal.route.accept();
                        return Err(ShareFetchTerminalSettlementError::Core(error.kind()));
                    }
                };
                if let Some(timeout_ms) = response_timeout {
                    self.commit_lock_timeout_ms(timeout_ms);
                }
                self.commit_throttle_until(throttle_until);
                self.staged = Some(StagedShareFetchDelivery {
                    fence: attempt.fence(),
                    route: terminal.route,
                    throttle_time_ms: decoded.throttle_time_ms,
                    endpoints: decoded.endpoints,
                    partitions: decoded.partitions,
                    acquisitions,
                });
                Ok(ShareFetchSettlementTurn::Acquired(acquisitions))
            }
            ShareFetchResolution::BrokerRejected(rejection) => {
                terminal.route.accept();
                self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                    .map_err(ShareFetchTerminalSettlementError::Session)?;
                Err(ShareFetchTerminalSettlementError::BrokerRejected(
                    rejection.error_code,
                ))
            }
            ShareFetchResolution::Failed { kind, delivery } => {
                terminal.route.accept();
                self.settle_attempt_failure(attempt, delivery)
                    .map_err(ShareFetchTerminalSettlementError::Session)?;
                Err(ShareFetchTerminalSettlementError::Driver { kind, delivery })
            }
        }
    }
}

fn lock_deadline(
    context: ShareFetchTerminalContext,
    timeout_ms: u32,
) -> Result<kafka_client_core::Deadline, ShareFetchTerminalSettlementError> {
    let ticks = u64::from(timeout_ms)
        .checked_mul(1_000_000)
        .ok_or(ShareFetchTerminalSettlementError::LockDeadlineOverflow)?;
    context
        .submitted_at
        .checked_deadline_after(ticks)
        .ok_or(ShareFetchTerminalSettlementError::LockDeadlineOverflow)
}

fn throttle_deadline(
    now: Moment,
    throttle_time_ms: u32,
) -> Result<kafka_client_core::Deadline, ShareFetchTerminalSettlementError> {
    let ticks = u64::from(throttle_time_ms)
        .checked_mul(1_000_000)
        .ok_or(ShareFetchTerminalSettlementError::ThrottleDeadlineOverflow)?;
    now.checked_deadline_after(ticks)
        .ok_or(ShareFetchTerminalSettlementError::ThrottleDeadlineOverflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchSettlementTurn {
    Acquired(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchTerminalSettlementError {
    Occupied,
    MissingTerminal,
    MissingLockTimeout,
    LockDeadlineOverflow,
    ThrottleDeadlineOverflow,
    Decode(ShareFetchAcquisitionDecodeError),
    Core(ShareFetchSettlementErrorKind),
    BrokerRejected(NonZeroI16),
    Driver {
        kind: ShareFetchFailureKind,
        delivery: DeliveryStatus,
    },
    Session(ShareFetchSessionOwnerError),
}

impl ShareFetchTerminalSettlementError {
    pub(in crate::consumer::share) const fn kind(&self) -> ShareFetchTerminalSettlementErrorKind {
        match self {
            Self::Occupied => ShareFetchTerminalSettlementErrorKind::Occupied,
            Self::MissingTerminal => ShareFetchTerminalSettlementErrorKind::MissingTerminal,
            Self::MissingLockTimeout => ShareFetchTerminalSettlementErrorKind::MissingLockTimeout,
            Self::LockDeadlineOverflow => {
                ShareFetchTerminalSettlementErrorKind::LockDeadlineOverflow
            }
            Self::ThrottleDeadlineOverflow => {
                ShareFetchTerminalSettlementErrorKind::ThrottleDeadlineOverflow
            }
            Self::Decode(_) => ShareFetchTerminalSettlementErrorKind::Decode,
            Self::Core(kind) => ShareFetchTerminalSettlementErrorKind::Core(*kind),
            Self::BrokerRejected(code) => {
                ShareFetchTerminalSettlementErrorKind::BrokerRejected(code.get())
            }
            Self::Driver { kind, delivery } => ShareFetchTerminalSettlementErrorKind::Driver {
                kind: *kind,
                delivery: *delivery,
            },
            Self::Session(error) => ShareFetchTerminalSettlementErrorKind::Session(*error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchTerminalSettlementErrorKind {
    Occupied,
    MissingTerminal,
    MissingLockTimeout,
    LockDeadlineOverflow,
    ThrottleDeadlineOverflow,
    Decode,
    Core(ShareFetchSettlementErrorKind),
    BrokerRejected(i16),
    Driver {
        kind: ShareFetchFailureKind,
        delivery: DeliveryStatus,
    },
    Session(ShareFetchSessionOwnerError),
}
