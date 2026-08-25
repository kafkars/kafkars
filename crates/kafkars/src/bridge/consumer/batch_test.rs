//! Private assigned-consumer batch translation shape contract.

use super::AssignedConsumerFetchEvidence;
use super::batch::{
    AssignedConsumerBatch, AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords,
};

#[test]
fn bridge_views_preserve_the_engine_borrowing_boundary() {
    fn batch_contract(batch: &AssignedConsumerBatch) {
        let _: &str = batch.topic();
        let _: i32 = batch.partition();
        let _: i64 = batch.checkpoint_next_offset();
        let _: AssignedConsumerFetchEvidence = batch.evidence();
        let _: usize = batch.record_count();
        let _: Option<AssignedConsumerRecord<'_>> = batch.records().next();
    }

    fn record_contract(record: &AssignedConsumerRecord<'_>) {
        let _: &str = record.topic();
        let _: i32 = record.partition();
        let _: i64 = record.offset();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Option<AssignedConsumerHeader<'_>> = record.headers().next();
    }

    fn iterator_contract<'batch>(mut records: AssignedConsumerRecords<'batch>) {
        let _: Option<AssignedConsumerRecord<'batch>> = records.next();
    }

    let _ = batch_contract as fn(&AssignedConsumerBatch);
    let _ = record_contract as fn(&AssignedConsumerRecord<'_>);
    let _ = iterator_contract as fn(AssignedConsumerRecords<'_>);
}
