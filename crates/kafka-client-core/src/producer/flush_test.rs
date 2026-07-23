//! Unit scenarios for bounded flush-ledger ownership.

use std::collections::BTreeMap;

use super::flush::{FlushLedger, FlushLedgerError};
use crate::{
    AdmissionSequence, BatchId, ByteCount, Deadline, FlushId, OperationId, ProducerEffect,
    ProducerOperation,
};

#[test]
fn empty_barrier_accepts_and_completes_in_one_transition() {
    let mut ledger = FlushLedger::new(1);
    let operations = BTreeMap::new();

    let effects = ledger
        .request(Some(OperationId::from_raw(1)), &operations)
        .unwrap_or_else(|error| panic!("empty flush request failed: {error}"));

    assert_eq!(
        effects,
        [
            ProducerEffect::AcceptFlush {
                flush_id: FlushId::from_raw(1),
                barrier: barrier(1),
            },
            ProducerEffect::CompleteFlush {
                flush_id: FlushId::from_raw(1),
            },
        ]
    );
    assert_eq!(ledger.len(), 1);
}

#[test]
fn pending_and_terminal_slots_obey_reclaim_and_capacity() {
    let mut ledger = FlushLedger::new(1);
    let mut operations = BTreeMap::new();
    operations.insert(
        OperationId::from_raw(1),
        ProducerOperation::admitted(
            OperationId::from_raw(1),
            Deadline::from_tick(100),
            ByteCount::new(1),
            BatchId::from_raw(1),
        ),
    );
    let effects = ledger
        .request(Some(OperationId::from_raw(2)), &operations)
        .unwrap_or_else(|error| panic!("pending flush request failed: {error}"));
    let [ProducerEffect::AcceptFlush { flush_id, .. }] = effects.as_slice() else {
        panic!("active operation must keep the flush pending")
    };

    assert_eq!(
        ledger.reclaim(*flush_id),
        Err(FlushLedgerError::NotCompleted)
    );
    assert_eq!(
        ledger.request(Some(OperationId::from_raw(2)), &operations),
        Err(FlushLedgerError::Capacity)
    );
}

const fn barrier(next: u128) -> AdmissionSequence {
    AdmissionSequence::from_raw(next)
}
