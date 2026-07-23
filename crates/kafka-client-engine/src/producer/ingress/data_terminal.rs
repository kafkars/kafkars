//! Shard-wide terminal checks and notification-owner transfer.

use kafka_client_core::Moment;

use crate::producer::{
    execution_stop::ProducerExecutionStopError, pending::PendingNotificationCleanupOwner,
    shutdown::ProducerNotifierRecovery,
};

use super::{
    data::ProducerShardData,
    terminal::{ProducerShardPendingOwnership, ProducerShardTerminalError},
};

impl ProducerShardData {
    pub(crate) fn execution_unavailable(
        &mut self,
        now: Moment,
    ) -> Result<(), ProducerExecutionStopError> {
        self.close_admission();
        self.host.execution_unavailable(now)
    }

    pub(crate) fn verify_release_before_completion(
        &self,
    ) -> Result<(), ProducerShardTerminalError> {
        self.require_empty_pending()?;
        self.host
            .verify_release_before_completion()
            .map_err(Into::into)
    }

    pub(crate) fn drain_terminal_mechanisms(&mut self) -> Result<(), ProducerShardTerminalError> {
        self.require_empty_pending()?;
        self.host.drain_terminal_mechanisms();
        Ok(())
    }

    pub(crate) fn verify_terminal_cleanup(&self) -> Result<(), ProducerShardTerminalError> {
        self.require_empty_pending()?;
        self.host.verify_terminal_cleanup().map_err(Into::into)
    }

    pub(crate) fn begin_notification_shutdown(
        &mut self,
    ) -> Result<PendingNotificationCleanupOwner, ProducerShardTerminalError> {
        self.require_empty_pending()?;
        self.host.begin_notification_shutdown().map_err(Into::into)
    }

    pub(crate) fn recover_notifier(
        &mut self,
    ) -> Result<ProducerNotifierRecovery, ProducerShardTerminalError> {
        self.require_empty_pending()?;
        self.host.recover_notifier().map_err(Into::into)
    }

    fn require_empty_pending(&self) -> Result<(), ProducerShardTerminalError> {
        if self.has_pending_fatal() {
            return Err(ProducerShardTerminalError::PendingFatal);
        }
        let pending = self.pending.stats();
        let ownership = ProducerShardPendingOwnership::new(
            pending.records,
            pending.retained_bytes,
            self.pending_notification_permits.in_use(),
        );
        if ownership.is_empty() {
            Ok(())
        } else {
            Err(ProducerShardTerminalError::Pending(ownership))
        }
    }
}
