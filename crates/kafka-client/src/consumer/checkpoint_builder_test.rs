//! Public partial-checkpoint API shape and stable error vocabulary.

use super::{
    Checkpoint, CheckpointBuilder, CheckpointMarkError, CheckpointMarkErrorKind, ConsumerBatch,
    GroupConsumerRecord,
};

#[test]
fn partial_checkpoint_surface_is_linear_and_record_borrowed() {
    fn builder(batch: &ConsumerBatch) -> CheckpointBuilder<'_> {
        batch.checkpoint_builder()
    }
    fn mark(
        builder: &mut CheckpointBuilder<'_>,
        record: &GroupConsumerRecord<'_>,
    ) -> Result<(), CheckpointMarkError> {
        builder.mark_processed(record)
    }
    fn finish(builder: CheckpointBuilder<'_>) -> Checkpoint {
        builder.finish()
    }
    let _: CheckpointMarkErrorKind = CheckpointMarkErrorKind::ForeignRecord;
    let _: CheckpointMarkErrorKind = CheckpointMarkErrorKind::OutOfOrder;
    let _ = builder as fn(&ConsumerBatch) -> CheckpointBuilder<'_>;
    let _ = mark as fn(
        &mut CheckpointBuilder<'_>,
        &GroupConsumerRecord<'_>,
    ) -> Result<(), CheckpointMarkError>;
    let _ = finish as fn(CheckpointBuilder<'_>) -> Checkpoint;
}
