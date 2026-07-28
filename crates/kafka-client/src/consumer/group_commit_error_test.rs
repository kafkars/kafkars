//! Public pre-admission group commit ownership contract.

use super::{Checkpoint, ConsumerCommitAdmissionError, ConsumerCommitError};

#[test]
fn rejection_exposes_and_returns_the_exact_checkpoint() {
    fn contract(error: ConsumerCommitAdmissionError) {
        let _: &crate::KafkaError = error.error();
        let _: &Checkpoint = error.checkpoint();
        let _: (Checkpoint, crate::KafkaError) = error.into_parts();
    }

    let _ = contract as fn(ConsumerCommitAdmissionError);
}

#[test]
fn terminal_error_exposes_and_returns_the_exact_retry_checkpoint() {
    fn contract(error: ConsumerCommitError) {
        let _: &crate::KafkaError = error.error();
        let _: Option<&Checkpoint> = error.checkpoint();
        let _: (Option<Checkpoint>, crate::KafkaError) = error.into_parts();
    }

    let _ = contract as fn(ConsumerCommitError);
}
