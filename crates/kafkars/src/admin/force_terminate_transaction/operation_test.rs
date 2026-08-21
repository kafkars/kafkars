//! Force-termination operation and singleton-result translation scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a specific terminal failure"
)]

use std::{future::Future, time::Duration};

use super::{ForceTerminateTransaction, operation::translate_fence_result};
use crate::{
    BatchResult, DeliveryStatus, ErrorKind, KafkaError,
    admin::{FenceProducersResult, FencedProducerIdentity},
};

#[test]
fn force_terminate_transaction_is_a_named_send_future() {
    fn assert_future<T: Future<Output = Result<(), KafkaError>> + Send>() {}
    assert_future::<ForceTerminateTransaction>();
}

#[test]
fn singleton_success_discards_only_the_fenced_identity() {
    let result = fence_result(vec![(
        String::from("orders-writer"),
        Ok(FencedProducerIdentity::new(41, 3)),
    )]);

    assert_eq!(translate_fence_result(Ok(result)), Ok(()));
}

#[test]
fn singleton_broker_failure_is_preserved_exactly() {
    let broker_error = KafkaError::new(ErrorKind::Broker, "transaction coordinator rejected fence")
        .with_broker_code(Some(-32_000))
        .with_delivery_status(DeliveryStatus::PossiblySent);
    let result = fence_result(vec![(
        String::from("orders-writer"),
        Err(broker_error.clone()),
    )]);

    assert_eq!(translate_fence_result(Ok(result)), Err(broker_error));
}

#[test]
fn non_singleton_terminal_is_an_internal_possibly_sent_failure() {
    for entries in [
        Vec::new(),
        vec![
            (
                String::from("orders-writer"),
                Ok(FencedProducerIdentity::new(41, 3)),
            ),
            (
                String::from("audit-writer"),
                Ok(FencedProducerIdentity::new(73, 5)),
            ),
        ],
    ] {
        let error = translate_fence_result(Ok(fence_result(entries)))
            .expect_err("non-singleton fencing terminal must fail");
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    }
}

fn fence_result(
    entries: Vec<(String, Result<FencedProducerIdentity, KafkaError>)>,
) -> FenceProducersResult {
    FenceProducersResult::new(Duration::ZERO, BatchResult::new(entries))
}
