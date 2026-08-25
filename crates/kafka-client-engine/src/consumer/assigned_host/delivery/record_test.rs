//! Borrowed record and header API-shape scenarios.

use bytes::Bytes;

use super::{AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords};

#[test]
fn record_views_borrow_bytes_and_preserve_nullable_header_access() {
    fn record_contract(record: &AssignedConsumerRecord<'_>) {
        let _: &str = record.topic();
        let _: i32 = record.partition();
        let _: i64 = record.offset();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Option<AssignedConsumerHeader<'_>> = record.headers().next();
    }

    fn header_contract(header: &AssignedConsumerHeader<'_>) {
        let _: &[u8] = header.key();
        let _: Option<&[u8]> = header.value();
    }

    fn shared_header_contract(header: AssignedConsumerHeader<'_>) -> (Bytes, Option<Bytes>) {
        header.into_shared_parts()
    }

    fn iterator_contract<'batch>(mut records: AssignedConsumerRecords<'batch>) {
        let _: Option<AssignedConsumerRecord<'batch>> = records.next();
    }

    let _ = record_contract as fn(&AssignedConsumerRecord<'_>);
    let _ = header_contract as fn(&AssignedConsumerHeader<'_>);
    let _ = shared_header_contract as fn(AssignedConsumerHeader<'_>) -> (Bytes, Option<Bytes>);
    let _ = iterator_contract as fn(AssignedConsumerRecords<'_>);
}
