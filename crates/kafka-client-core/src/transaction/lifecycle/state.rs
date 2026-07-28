//! Closed stages for one uniquely owned transactional producer.

/// Current transaction lifecycle stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleState {
    /// The producer owns no active transaction.
    Idle,
    /// One healthy transaction accepts sends and may commit or abort.
    Active,
    /// One transaction rejects new sends and may only abort.
    AbortRequired,
    /// Abort was requested and accepted sends are being terminally drained.
    DrainingAbort,
    /// One explicit commit request awaits settlement.
    EndingCommit,
    /// One explicit or best-effort abort request awaits settlement.
    EndingAbort,
    /// The producer is irrecoverably fenced.
    Fatal,
    /// A lost fatal owner is draining accepted sends without submitting `EndTxn`.
    DrainingFatal,
    /// The external owner was released and no success can be observed.
    Closed,
}
