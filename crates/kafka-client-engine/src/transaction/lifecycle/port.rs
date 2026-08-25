//! Narrow transaction-end submission and terminal polling seam.

use std::time::Instant;

use kafka_client_core::{TransactionEndFailure, TransactionEndMode};

/// Exact immutable facts needed to submit one `EndTxn`.
pub(super) struct TransactionEndRequest<'a> {
    pub(super) transactional_id: &'a str,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) mode: TransactionEndMode,
    pub(super) deadline: Instant,
}

/// Closed lifecycle consequence of a driver terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionEndPortTerminal {
    Succeeded,
    RetryableCoordinatorLoss(TransactionEndFailure),
    Failed(TransactionEndFailure),
}

/// Linear driver evidence retained until deterministic settlement accepts it.
pub(super) trait TransactionEndPortTerminalEvidence {
    fn terminal(&self) -> TransactionEndPortTerminal;

    fn discard(self: Box<Self>);
}

/// One bounded observation of an accepted `EndTxn` call or its causal refresh.
pub(super) enum TransactionEndPortCallPoll {
    Pending,
    DeadlineElapsed(Box<dyn TransactionEndPortTerminalEvidence>),
    Terminal(Box<dyn TransactionEndPortTerminalEvidence>),
}

/// One accepted call retained until exactly one terminal.
pub(super) trait TransactionEndPortCall: Send {
    fn poll(&mut self, deadline_elapsed: bool) -> TransactionEndPortCallPoll;

    fn recover_after_driver_shutdown(self: Box<Self>) -> TransactionEndFailure;
}

/// Private fakeable boundary around concrete routed transaction calls.
pub(super) trait TransactionEndPort {
    fn submit(
        &mut self,
        request: TransactionEndRequest<'_>,
    ) -> Result<Box<dyn TransactionEndPortCall>, TransactionEndFailure>;
}
