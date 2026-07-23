//! Exact completion identity and front-only FIFO scenarios.

use kafka_client_core::{DeliveryStatus, OperationId, ProducerCompletion, ProducerFailure};

use crate::completion::CompletionId;

use super::terminal_backlog::{OrderedTerminalBacklog, RetainedTerminal};

#[test]
fn record_terminals_leave_the_bounded_fifo_only_from_the_front() {
    let mut backlog = OrderedTerminalBacklog::new(2);
    backlog.push(retained(1, 0));
    backlog.push(retained(2, 1));

    assert_operation(backlog.front(), 1);
    let Some(first) = backlog.pop_published() else {
        panic!("first terminal should remain owned")
    };
    assert_operation(Some(&first), 1);
    assert_operation(backlog.front(), 2);
    assert_eq!(backlog.len(), 1);
}

fn retained(operation: u64, slot: usize) -> RetainedTerminal {
    RetainedTerminal::new(
        OperationId::from_raw(operation),
        CompletionId::from_parts_for_test(slot, 1),
        terminal(),
    )
}

fn terminal() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}

fn assert_operation(terminal: Option<&RetainedTerminal>, expected: u64) {
    let Some(terminal) = terminal else {
        panic!("backlog must contain one exact record terminal")
    };
    assert_eq!(terminal.operation_id().get(), expected);
}
