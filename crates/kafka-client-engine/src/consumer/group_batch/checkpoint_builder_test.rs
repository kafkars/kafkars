//! Exact-batch identity and ordered-prefix checkpoint scenarios.

use super::{GroupConsumerCheckpointMarkErrorKind, test_support::GroupBatchFixture};

#[test]
fn builder_rejects_skips_duplicates_and_foreign_records_without_advancing() {
    let mut fixture = GroupBatchFixture::start();
    let mut foreign_fixture = GroupBatchFixture::start();
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    let foreign_batch = foreign_fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("foreign batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready foreign group batch"));
    let records = batch.records().collect::<Vec<_>>();
    let foreign = foreign_batch
        .records()
        .next()
        .unwrap_or_else(|| panic!("foreign record"));
    let mut builder = batch.checkpoint_builder();

    assert_eq!(
        builder.mark_processed(&records[1]).unwrap_err().kind(),
        GroupConsumerCheckpointMarkErrorKind::OutOfOrder
    );
    assert_eq!(
        builder.mark_processed(&foreign).unwrap_err().kind(),
        GroupConsumerCheckpointMarkErrorKind::ForeignRecord
    );
    builder
        .mark_processed(&records[0])
        .unwrap_or_else(|error| panic!("mark first record: {error}"));
    assert_eq!(
        builder.mark_processed(&records[0]).unwrap_err().kind(),
        GroupConsumerCheckpointMarkErrorKind::OutOfOrder
    );
    builder
        .mark_processed(&records[1])
        .unwrap_or_else(|error| panic!("mark second record: {error}"));
    let checkpoint = builder.finish();
    assert_eq!(checkpoint.next_offset(), 19);
    let core = checkpoint.into_core();
    assert_eq!(core.group_id(), fixture.group_id);
    assert_eq!(core.entries()[0].next_offset(), 19);

    drop(records);
    drop(batch);
    drop(foreign);
    drop(foreign_batch);
    fixture.finish();
    foreign_fixture.finish();
}

#[test]
fn empty_prefix_and_full_batch_paths_remain_safe_and_distinct() {
    let mut fixture = GroupBatchFixture::start();
    let batch = fixture
        .handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("batch observation: {error}"))
        .unwrap_or_else(|| panic!("ready group batch"));
    let checkpoint = batch.checkpoint_builder().finish();
    assert_eq!(checkpoint.next_offset(), 17);
    drop(checkpoint);
    assert_eq!(batch.checkpoint_next_offset(), 20);
    assert_eq!(batch.into_checkpoint().next_offset(), 20);
    fixture.finish();
}
