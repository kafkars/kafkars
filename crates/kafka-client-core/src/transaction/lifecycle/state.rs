//! Closed stages for one uniquely owned transactional producer.

/// Current transaction lifecycle stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLifecycleState {
    /// The producer owns no active transaction.
    Idle,
    /// One healthy transaction may commit or abort.
    Active,
    /// One explicit commit request awaits settlement.
    EndingCommit,
    /// One explicit or best-effort abort request awaits settlement.
    EndingAbort,
    /// The producer is irrecoverably fenced.
    Fatal,
    /// The external owner was released and no success can be observed.
    Closed,
}
