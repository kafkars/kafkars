//! Forbidden duplicable completion-ledger fixture.

#[derive(Clone, Copy)]
struct CompletionLedger {
    slots: usize,
}

#[derive(Clone)]
enum WaitingOutcome {
    Pending,
}
