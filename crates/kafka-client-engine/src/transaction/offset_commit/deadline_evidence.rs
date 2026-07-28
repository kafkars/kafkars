//! Deadline settlement for coordinator-refresh calls after accepted offset RPCs.

use kafka_client_core::{
    DeliveryStatus, TransactionOffsetCommitConsequence, TransactionOffsetCommitStage,
};

use super::{
    model::TransactionOffsetCommitFailureKind,
    port::{TransactionOffsetCommitPortEvidence, TransactionOffsetCommitPortFact},
};

pub(super) fn deadline_evidence(
    source: Box<dyn TransactionOffsetCommitPortEvidence>,
) -> Box<dyn TransactionOffsetCommitPortEvidence> {
    Box::new(TransactionOffsetCommitDeadlineEvidence { source })
}

struct TransactionOffsetCommitDeadlineEvidence {
    source: Box<dyn TransactionOffsetCommitPortEvidence>,
}

impl TransactionOffsetCommitPortEvidence for TransactionOffsetCommitDeadlineEvidence {
    fn correlation(
        &self,
    ) -> (
        kafka_client_core::TransactionEpoch,
        kafka_client_core::TransactionOffsetCommitId,
        TransactionOffsetCommitStage,
    ) {
        self.source.correlation()
    }

    fn fact(&self) -> TransactionOffsetCommitPortFact {
        match self.source.fact() {
            TransactionOffsetCommitPortFact::RetryableCoordinatorLoss { delivery, .. } => {
                TransactionOffsetCommitPortFact::Failed {
                    consequence: TransactionOffsetCommitConsequence::AbortRequired,
                    kind: TransactionOffsetCommitFailureKind::DeadlineElapsed,
                    delivery,
                }
            }
            TransactionOffsetCommitPortFact::Failed {
                consequence,
                delivery,
                ..
            } => TransactionOffsetCommitPortFact::Failed {
                consequence,
                kind: TransactionOffsetCommitFailureKind::DeadlineElapsed,
                delivery,
            },
            TransactionOffsetCommitPortFact::Succeeded => TransactionOffsetCommitPortFact::Failed {
                consequence: TransactionOffsetCommitConsequence::Fatal,
                kind: TransactionOffsetCommitFailureKind::Correlation,
                delivery: DeliveryStatus::PossiblySent,
            },
        }
    }

    fn discard(self: Box<Self>) {
        self.source.discard();
    }
}
