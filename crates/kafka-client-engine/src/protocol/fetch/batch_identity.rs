//! Exact producer tuple and control-batch identity validation.

use super::{failure::FetchDecodeFailure, model::FetchProducerIdentity};

pub(super) fn producer_identity(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional: bool,
    control: bool,
) -> Result<Option<FetchProducerIdentity>, FetchDecodeFailure> {
    let identity = match (producer_id, producer_epoch, base_sequence) {
        (-1, -1, -1) => None,
        (producer_id, producer_epoch, base_sequence)
            if producer_id >= 0
                && producer_epoch >= 0
                && ((!control && base_sequence >= 0) || (control && base_sequence == -1)) =>
        {
            Some(FetchProducerIdentity {
                producer_id,
                producer_epoch,
                base_sequence,
            })
        }
        _ => {
            return Err(FetchDecodeFailure::InvalidProducerIdentity {
                producer_id,
                producer_epoch,
                base_sequence,
            });
        }
    };
    if control && !transactional {
        return Err(FetchDecodeFailure::ControlBatchNotTransactional);
    }
    if transactional && identity.is_none() {
        return Err(FetchDecodeFailure::TransactionalIdentityMissing);
    }
    Ok(identity)
}
