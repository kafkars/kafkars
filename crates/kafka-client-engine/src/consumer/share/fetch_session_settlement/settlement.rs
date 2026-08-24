//! Atomic `ShareFetch` terminal interpretation and decoded delivery staging.

use kafka_client_core::{DeliveryStatus, Moment};

use crate::driver::{ShareFetchResolution, ShareFetchTerminalContext};

use super::super::{
    fetch_acquisition_decode::{ShareFetchAcquisitionDecodeError, decode_share_fetch_success},
    fetch_session::ShareFetchSessionOwner,
};
use super::recovery::{
    ShareFetchResponseRecovery, broker_recovery, driver_recovery, response_recovery, route_recovery,
};
use super::terminal::{
    ShareFetchSettlementTurn, ShareFetchTerminalSettlementError, StagedShareFetchDelivery,
};

impl ShareFetchSessionOwner {
    #[expect(
        clippy::too_many_lines,
        reason = "one atomic terminal transition retains route certainty through recovery or delivery staging"
    )]
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
                match response_recovery(&success) {
                    ShareFetchResponseRecovery::Session => {
                        terminal.route.accept();
                        self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                            .map_err(ShareFetchTerminalSettlementError::Session)?;
                        return Ok(ShareFetchSettlementTurn::Recover(
                            super::super::fetch_session_set::ShareFetchSessionRecovery::session(),
                        ));
                    }
                    ShareFetchResponseRecovery::Route(kafka_topic_id) => {
                        let Some((topic, observed)) = self
                            .request_plan()
                            .route_refresh_requirement(kafka_topic_id)
                        else {
                            terminal.route.accept();
                            self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                                .map_err(ShareFetchTerminalSettlementError::Session)?;
                            return Err(ShareFetchTerminalSettlementError::Decode(
                                ShareFetchAcquisitionDecodeError::PartitionRejected,
                            ));
                        };
                        let recovery = route_recovery(
                            terminal.route,
                            attempt,
                            terminal.capture,
                            now,
                            topic,
                            observed,
                        );
                        let recovery = match recovery {
                            Ok(recovery) => recovery,
                            Err(route) => {
                                route.accept();
                                self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                                    .map_err(ShareFetchTerminalSettlementError::Session)?;
                                return Err(ShareFetchTerminalSettlementError::Decode(
                                    ShareFetchAcquisitionDecodeError::PartitionRejected,
                                ));
                            }
                        };
                        self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                            .map_err(ShareFetchTerminalSettlementError::Session)?;
                        return Ok(ShareFetchSettlementTurn::Recover(recovery));
                    }
                    ShareFetchResponseRecovery::None | ShareFetchResponseRecovery::Terminal => {}
                }
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
                if acquisitions == 0 {
                    terminal.route.accept();
                    drop(decoded.endpoints);
                    drop(decoded.partitions);
                    return Ok(ShareFetchSettlementTurn::Empty);
                }
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
                if let Some(recovery) = broker_recovery(rejection.error_code) {
                    terminal.route.accept();
                    self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                        .map_err(ShareFetchTerminalSettlementError::Session)?;
                    return Ok(ShareFetchSettlementTurn::Recover(recovery));
                }
                terminal.route.accept();
                self.settle_attempt_failure(attempt, DeliveryStatus::PossiblySent)
                    .map_err(ShareFetchTerminalSettlementError::Session)?;
                Err(ShareFetchTerminalSettlementError::BrokerRejected(
                    rejection.error_code,
                ))
            }
            ShareFetchResolution::Failed { kind, delivery } => {
                let recovery = driver_recovery(
                    terminal.route,
                    attempt,
                    terminal.context.submitted_at,
                    now,
                    kind,
                );
                let recovery = match recovery {
                    Ok(recovery) => Some(recovery),
                    Err(route) => {
                        route.accept();
                        None
                    }
                };
                self.settle_attempt_failure(attempt, delivery)
                    .map_err(ShareFetchTerminalSettlementError::Session)?;
                if let Some(recovery) = recovery {
                    return Ok(ShareFetchSettlementTurn::Recover(recovery));
                }
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
