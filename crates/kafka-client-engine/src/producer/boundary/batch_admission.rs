//! Linear batch permit from validated caller ownership through shard admission.

use crate::{
    clock::MonotonicClock,
    producer::ingress::ProducerBatchAdmissionPermit as PortBatchAdmissionPermit,
};

use super::{
    ProducerBatchSendCapture, ProducerTrySendBatch, ProducerTrySendBatchError,
    ProducerTrySendError, ProducerTrySendErrorKind,
    prepare::{ValidatedBatch, validate_batch},
    record::ProducerRecord,
    result::ProducerTrySendAccepted,
};

/// One nonblocking shard acquisition for a caller-validated producer batch.
#[must_use = "a producer batch admission permit must be consumed immediately"]
pub struct ProducerBatchAdmission<'a> {
    permit: PortBatchAdmissionPermit<'a>,
    clock: &'a MonotonicClock,
}

impl<'a> ProducerBatchAdmission<'a> {
    pub(super) const fn new(
        permit: PortBatchAdmissionPermit<'a>,
        clock: &'a MonotonicClock,
    ) -> Self {
        Self { permit, clock }
    }

    /// Validates and admits one engine-native batch through this acquired shard.
    pub fn try_send_captured(
        self,
        capture: ProducerBatchSendCapture,
        records: Vec<ProducerRecord>,
    ) -> ProducerTrySendBatch {
        if records.is_empty() {
            return ProducerTrySendBatch::new(Vec::new(), None);
        }
        let validated = match validate_batch(capture, records) {
            Ok(validated) => validated,
            Err(rejected) => {
                let (kind, records) = rejected.into_parts();
                return rejected_batch(kind, records);
            }
        };
        self.admit_validated(validated)
    }

    pub(super) fn admit_validated(self, validated: ValidatedBatch) -> ProducerTrySendBatch {
        let (_boundary_at, deadline, records) = validated.into_prepared().into_parts();
        let attempted_at = match self.clock.now() {
            Ok(now) => now,
            Err(_error) => {
                let records = records
                    .into_iter()
                    .map(ProducerRecord::from_stored)
                    .collect();
                return rejected_batch(ProducerTrySendErrorKind::DeadlineUnrepresentable, records);
            }
        };
        let admitted = self.permit.admit(attempted_at, deadline, records);
        let (accepted, rejection) = admitted.into_parts();
        let accepted = accepted
            .into_iter()
            .map(ProducerTrySendAccepted::from_port)
            .collect();
        let rejection = rejection.map(|rejection| {
            let (first, remaining) = rejection.into_parts();
            let remaining = remaining
                .into_iter()
                .map(ProducerRecord::from_stored)
                .collect();
            ProducerTrySendBatchError::from_single(
                ProducerTrySendError::from_port(first),
                remaining,
            )
        });
        ProducerTrySendBatch::new(accepted, rejection)
    }
}

pub(super) fn rejected_batch(
    kind: ProducerTrySendErrorKind,
    records: Vec<ProducerRecord>,
) -> ProducerTrySendBatch {
    ProducerTrySendBatch::new(
        Vec::new(),
        Some(ProducerTrySendBatchError::from_parts(kind, records, None)),
    )
}

impl std::fmt::Debug for ProducerBatchAdmission<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerBatchAdmission")
            .finish_non_exhaustive()
    }
}
