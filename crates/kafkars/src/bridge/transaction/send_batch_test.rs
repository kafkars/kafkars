//! Private homogeneous transactional batch ownership and observation contracts.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, Record, header_name::SourceOwner,
    record::RecordTransferParts, transaction::TransactionBatchMetadata,
};

use super::{TransactionBatchSendEngine, TransactionEngine, send_batch::prepare_engine_records};

type SendBatchMethod<'send, 'producer> =
    fn(
        &'send mut TransactionEngine<'producer>,
        Vec<Record>,
        Duration,
    ) -> Result<TransactionBatchSendEngine<'send, 'producer>, (Vec<Record>, KafkaError)>;

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn over_capacity_batch_returns_exact_facade_records_without_touching_source_owners() {
    let first_dropped = Arc::new(AtomicBool::new(false));
    let second_dropped = Arc::new(AtomicBool::new(false));
    let records = vec![
        source_owned_record("first", Arc::clone(&first_dropped)),
        source_owned_record("second", Arc::clone(&second_dropped)),
    ];

    let Err((records, error)) = prepare_engine_records(records, 1) else {
        panic!("over-capacity facade batch was unexpectedly converted")
    };

    assert_eq!(error.kind(), ErrorKind::Backpressure);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].value_bytes(),
        Some(&Bytes::from_static(b"first"))
    );
    assert_eq!(
        records[1].value_bytes(),
        Some(&Bytes::from_static(b"second"))
    );
    assert!(!first_dropped.load(Ordering::Acquire));
    assert!(!second_dropped.load(Ordering::Acquire));
    drop(records);
    assert!(first_dropped.load(Ordering::Acquire));
    assert!(second_dropped.load(Ordering::Acquire));
}

fn source_owned_record(value: &'static str, dropped: Arc<AtomicBool>) -> Record {
    let source_owner: Arc<dyn Send + Sync> = Arc::new(DropSentinel(dropped));
    Record::from_transfer_parts(RecordTransferParts {
        topic: Arc::from("orders"),
        partition: Some(2),
        timestamp_milliseconds: None,
        key: None,
        value: Some(Bytes::from_static(value.as_bytes())),
        headers: Vec::new(),
        source_owner: SourceOwner::new(source_owner),
    })
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn private_batch_reborrows_transaction_through_one_runtime_neutral_observer() {
    fn require_send_batch(_method: SendBatchMethod<'_, '_>) {}
    fn require_future<T: Future<Output = Result<TransactionBatchMetadata, KafkaError>>>() {}
    fn require_wait(
        _method: fn(
            TransactionBatchSendEngine<'static, 'static>,
        ) -> Result<TransactionBatchMetadata, KafkaError>,
    ) {
    }

    require_send_batch(TransactionEngine::send_batch);
    require_future::<TransactionBatchSendEngine<'static, 'static>>();
    require_wait(TransactionBatchSendEngine::wait);
    assert_not_impl!(TransactionBatchSendEngine<'static, 'static>: Clone);
    assert_not_impl!(TransactionBatchSendEngine<'static, 'static>: Copy);
}
