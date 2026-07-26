//! Lossless core-to-engine offset-alteration terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterConsumerGroupOffsetBrokerError as CoreBrokerError,
    AlterConsumerGroupOffsetOutcome as CoreOutcome, AlterConsumerGroupOffsetsBatch as CoreBatch,
    AlterConsumerGroupOffsetsTerminal as CoreTerminal,
};

use super::{AlterConsumerGroupOffsetsOutcome, outcome::translate_terminal};

#[test]
fn throttle_caller_order_and_exact_partition_code_translate_once() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = CoreTerminal::Altered(CoreBatch::new(
        73,
        vec![
            CoreOutcome::altered("orders".to_owned(), 2),
            CoreOutcome::failed("audit".to_owned(), 0, CoreBrokerError::new(code)),
        ],
    ));
    let AlterConsumerGroupOffsetsOutcome::Altered(batch) = translate_terminal(terminal) else {
        panic!("altered batch expected");
    };
    let (throttle, results) = batch.into_parts();
    assert_eq!(throttle, 73);
    let (topic, partition, result) = results[0].clone().into_parts();
    assert_eq!((topic.as_str(), partition, result), ("orders", 2, Ok(())));
    let (topic, partition, result) = results[1].clone().into_parts();
    assert_eq!((topic.as_str(), partition), ("audit", 0));
    assert_eq!(
        result
            .err()
            .unwrap_or_else(|| panic!("broker rejection expected"))
            .code(),
        -31_999
    );
}
