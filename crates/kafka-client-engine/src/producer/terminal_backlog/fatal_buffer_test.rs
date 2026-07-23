//! Fatal transition buffer capacity and ordering scenarios.

use std::collections::VecDeque;

use kafka_client_core::{BatchId, ProducerEffect, ProducerInput};

use super::FatalTransitionBuffer;

#[test]
fn capture_uses_preallocated_capacity_and_preserves_current_before_tail() {
    let current = ProducerEffect::ReleaseBatch {
        batch_id: BatchId::from_raw(1),
    };
    let tail = [ProducerEffect::ReleaseBatch {
        batch_id: BatchId::from_raw(2),
    }];
    let generated = VecDeque::from([ProducerInput::ExecutionUnavailable]);
    let mut buffer = FatalTransitionBuffer::new(3);

    assert!(buffer.capture(Some(current), &tail, &generated, None));
    assert_eq!(buffer.take_effects(), [current, tail[0]]);
    assert_eq!(
        buffer.take_generated(),
        [ProducerInput::ExecutionUnavailable]
    );
    assert!(!buffer.capture(Some(current), &tail, &generated, None));
    assert_eq!(buffer.retained_len(), 0);
}

#[test]
fn capture_refuses_over_capacity_without_partial_mutation() {
    let effects = [
        ProducerEffect::ReleaseBatch {
            batch_id: BatchId::from_raw(1),
        },
        ProducerEffect::ReleaseBatch {
            batch_id: BatchId::from_raw(2),
        },
    ];
    let mut buffer = FatalTransitionBuffer::new(1);

    assert!(!buffer.capture(None, &effects, &VecDeque::new(), None));
    assert_eq!(buffer.retained_len(), 0);
}
