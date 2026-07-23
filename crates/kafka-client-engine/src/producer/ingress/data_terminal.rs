//! Shard-wide terminal checks and completion-notifier ownership transfer.

use kafka_client_core::Moment;

use crate::{
    completion::NotifierJoin,
    producer::{execution_stop::ProducerExecutionStopError, shutdown::ProducerNotifierRecovery},
};

use super::{data::ProducerShardData, terminal::ProducerShardTerminalError};

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
        self.host
            .verify_release_before_completion()
            .map_err(Into::into)
    }

    pub(crate) fn drain_terminal_mechanisms(&mut self) {
        self.host.drain_terminal_mechanisms();
    }

    pub(crate) fn verify_terminal_cleanup(&self) -> Result<(), ProducerShardTerminalError> {
        self.host.verify_terminal_cleanup().map_err(Into::into)
    }

    pub(crate) fn begin_notification_shutdown(
        &mut self,
    ) -> Result<NotifierJoin, ProducerShardTerminalError> {
        self.host.begin_notification_shutdown().map_err(Into::into)
    }

    pub(crate) fn recover_notifier(&mut self) -> ProducerNotifierRecovery {
        self.host.recover_notifier()
    }
}
