//! Public-boundary transfer into the bounded producer waiting partition.

use super::ProducerHandle;
use crate::producer::boundary::{
    ProducerSendCapture, ProducerTrySendAccepted, ProducerTrySendError, prepare::prepare_waiting,
    record::ProducerRecord,
};

impl ProducerHandle {
    /// Enqueues one explicit-or-automatic call in the bounded FIFO waiting partition.
    ///
    /// This is a single admission attempt, never a retry loop. The returned
    /// observer owns cancellation-before-promotion and ordinary delivery after
    /// promotion.
    #[allow(
        clippy::result_large_err,
        reason = "pre-admission failures return the intact bytes-native record"
    )]
    pub fn send_captured(
        &self,
        capture: ProducerSendCapture,
        record: ProducerRecord,
    ) -> Result<ProducerTrySendAccepted, ProducerTrySendError> {
        let prepared = prepare_waiting(capture, record)?;
        let (attempted_at, deadline, stored) = prepared.into_parts();
        match self.port.admit_waiting(attempted_at, deadline, stored) {
            Ok(accepted) => Ok(ProducerTrySendAccepted::from_waiting_port(accepted)),
            Err(error) => Err(ProducerTrySendError::from_port(error)),
        }
    }
}
