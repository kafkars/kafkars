//! Private assigned-consumer Fetch evidence translation shape contract.

use super::AssignedConsumerFetchEvidence;

#[test]
fn bridge_evidence_preserves_every_engine_fact_without_generated_types() {
    fn contract(evidence: &AssignedConsumerFetchEvidence) {
        let _: &str = evidence.topic();
        let _: [u8; 16] = evidence.topic_uuid();
        let _: i32 = evidence.partition();
        let _: i64 = evidence.requested_offset();
        let _: i64 = evidence.next_offset();
        let _: Option<i64> = evidence.log_start_offset();
        let _: Option<i64> = evidence.last_stable_offset();
        let _: Option<i64> = evidence.high_watermark();
        let _: usize = evidence.retained_bytes();
    }

    let _ = contract as fn(&AssignedConsumerFetchEvidence);
}
