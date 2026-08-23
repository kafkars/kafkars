//! Atomic `ShareFetch` terminal interpretation and decoded delivery staging.

use core::num::NonZeroI16;

use kafka_client_core::{DeliveryStatus, Moment, ShareFetchSettlementErrorKind};

use crate::{
    driver::{
        ShareFetchFailureKind, ShareFetchResolution, ShareFetchRoute, ShareFetchTerminalContext,
    },
    protocol::consumer::share_fetch::ShareFetchEndpoint,
};

use super::{
    fetch_acquisition_decode::{
        DecodedShareFetchPartition, ShareFetchAcquisitionDecodeError, decode_share_fetch_success,
    },
    fetch_session::{ShareFetchSessionOwner, ShareFetchSessionOwnerError},
};

/// Decoded records and route receipt retained after atomic core admission.
#[must_use = "staged share delivery must be exposed or released"]
pub(super) struct StagedShareFetchDelivery {
    pub(super) route: ShareFetchRoute,
    pub(super) throttle_time_ms: u32,
    pub(super) endpoints: Vec<ShareFetchEndpoint>,
    pub(super) partitions: Vec<DecodedShareFetchPartition>,
    pub(super) acquisitions: usize,
}

impl ShareFetchSessionOwner {
    pub(super) fn settle_terminal(
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
                self.staged = Some(StagedShareFetchDelivery {
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

    pub(super) fn take_staged_delivery(&mut self) -> Option<StagedShareFetchDelivery> {
        self.staged.take()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSettlementTurn {
    Acquired(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchTerminalSettlementError {
    Occupied,
    MissingTerminal,
    MissingLockTimeout,
    LockDeadlineOverflow,
    Decode(ShareFetchAcquisitionDecodeError),
    Core(ShareFetchSettlementErrorKind),
    BrokerRejected(NonZeroI16),
    Driver {
        kind: ShareFetchFailureKind,
        delivery: DeliveryStatus,
    },
    Session(ShareFetchSessionOwnerError),
}
