//! Public borrowed record and header API-shape scenarios.

use super::{ConsumerHeader, ConsumerRecord, ConsumerRecords};

#[test]
fn record_views_preserve_nullable_bytes_and_header_iteration_shapes() {
    fn record_contract(record: &ConsumerRecord<'_>) {
        let _: &str = record.topic();
        let _: i32 = record.partition();
        let _: i64 = record.offset();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Option<ConsumerHeader<'_>> = record.headers().next();
    }

    fn header_contract(header: &ConsumerHeader<'_>) {
        let _: &[u8] = header.key();
        let _: Option<&[u8]> = header.value();
    }

    fn iterator_contract<'batch>(mut records: ConsumerRecords<'batch>) {
        let _: Option<ConsumerRecord<'batch>> = records.next();
    }

    let _ = record_contract as fn(&ConsumerRecord<'_>);
    let _ = header_contract as fn(&ConsumerHeader<'_>);
    let _ = iterator_contract as fn(ConsumerRecords<'_>);
}
