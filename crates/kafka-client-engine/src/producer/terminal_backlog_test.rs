//! Exact terminal retention through the bounded publication FIFO.

use kafka_client_core::{
    DeliveryStatus, FlushId, OperationId, ProducerCompletion, ProducerFailure,
};

use crate::completion::CompletionId;

use super::terminal_backlog::{OrderedTerminalBacklog, ProducerTerminalOwner, RetainedTerminal};

#[test]
fn record_terminals_leave_the_bounded_fifo_only_from_the_front() {
    let mut backlog = OrderedTerminalBacklog::new(2);
    backlog.push(record_terminal(1, 0));
    backlog.push(RetainedTerminal::flush(
        FlushId::from_raw(3),
        CompletionId::from_parts_for_test(1, 1),
    ));

    assert_eq!(
        backlog.front().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Record(OperationId::from_raw(1)))
    );
    let Some(first) = backlog.pop_published() else {
        panic!("first terminal should remain owned")
    };
    assert_eq!(
        first.owner(),
        ProducerTerminalOwner::Record(OperationId::from_raw(1))
    );
    assert_eq!(
        backlog.front().map(RetainedTerminal::owner),
        Some(ProducerTerminalOwner::Flush(FlushId::from_raw(3)))
    );
    assert_eq!(backlog.len(), 1);
}

fn record_terminal(operation: u64, slot: usize) -> RetainedTerminal {
    RetainedTerminal::record(
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
