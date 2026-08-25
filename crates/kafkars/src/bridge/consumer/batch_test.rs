//! Private assigned-consumer batch translation shape contract.

use super::batch::{
    AssignedConsumerBatch, AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords,
};
use super::{
    AssignedConsumerFetchEvidence, AssignedConsumerOwnedHeader, AssignedConsumerOwnedRecord,
};

#[test]
fn bridge_views_preserve_the_engine_borrowing_boundary() {
    type HeaderParts<'record> = (&'record [u8], Option<&'record [u8]>);
    type BorrowedHeaderContract =
        for<'record> fn(AssignedConsumerHeader<'record>) -> HeaderParts<'record>;
    type OwnedHeaderContract =
        for<'record> fn(AssignedConsumerOwnedHeader<'record>) -> HeaderParts<'record>;
    type OwnedHeaderCollector =
        for<'record> fn(&'record AssignedConsumerOwnedRecord) -> Vec<HeaderParts<'record>>;

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

    #[expect(
        clippy::needless_pass_by_value,
        reason = "consuming the iterator item proves returned references retain the record lifetime"
    )]
    fn retained_header_contract(header: AssignedConsumerHeader<'_>) -> HeaderParts<'_> {
        let key = header.key();
        let value = header.value();
        (key, value)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "consuming the iterator item proves returned references retain the record lifetime"
    )]
    fn owned_header_contract(header: AssignedConsumerOwnedHeader<'_>) -> HeaderParts<'_> {
        let key = header.key();
        let value = header.value();
        (key, value)
    }

    fn collect_owned_headers(record: &AssignedConsumerOwnedRecord) -> Vec<HeaderParts<'_>> {
        record
            .headers()
            .map(|header| (header.key(), header.value()))
            .collect()
    }

    let _ = batch_contract as fn(&AssignedConsumerBatch);
    let _ = record_contract as fn(&AssignedConsumerRecord<'_>);
    let _ = iterator_contract as fn(AssignedConsumerRecords<'_>);
    let _: BorrowedHeaderContract = retained_header_contract;
    let _: OwnedHeaderContract = owned_header_contract;
    let _: OwnedHeaderCollector = collect_owned_headers;
}
