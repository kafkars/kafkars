//! Runtime-neutral observation of one explicit transaction end.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_core::TransactionLifecycleTerminal;

use crate::completion::CompletionObserver;

mod translation;

use translation::observer_error;
pub(super) use translation::translate_terminal;

/// Exact terminal consequence of an accepted explicit transaction end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndOutcome {
    /// Kafka committed the transaction.
    Committed,
    /// Kafka aborted the transaction.
    Aborted,
    /// Transaction execution failed and the owner became permanently unusable.
    Failed(TransactionEndFailure),
}

/// Exact commit-or-abort intent retained by a failed end operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndIntent {
    /// The accepted operation requested commit.
    Commit,
    /// The accepted operation requested abort.
    Abort,
}

/// Stable cause of a failed accepted transaction end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the request before transport ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The negotiated request was incompatible with Kafka.
    Compatibility,
    /// Kafka returned a malformed or uncorrelatable response.
    InvalidResponse,
    /// The driver or its completion owner closed.
    DriverClosed,
    /// An internal request or completion fence did not correlate.
    Correlation,
    /// Authentication or authorization rejected the transactional identity.
    Access,
    /// The transaction coordinator could not serve the request.
    Coordinator,
    /// Kafka explicitly fenced the transactional producer identity.
    Fenced,
    /// Kafka returned another exact signed rejection.
    Broker,
    /// A prior transaction operation fenced the lifecycle before `EndTxn`.
    Lifecycle,
}

/// Exact failure facts retained through one terminal observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionEndFailure {
    kind: TransactionEndFailureKind,
    intent: TransactionEndIntent,
    delivery: TransactionEndDeliveryStatus,
    broker_code: Option<i16>,
}

impl TransactionEndFailure {
    /// Returns the stable terminal cause.
    pub const fn kind(self) -> TransactionEndFailureKind {
        self.kind
    }

    /// Returns whether the accepted operation requested commit or abort.
    pub const fn intent(self) -> TransactionEndIntent {
        self.intent
    }

    /// Returns authoritative transport certainty.
    pub const fn delivery(self) -> TransactionEndDeliveryStatus {
        self.delivery
    }

    /// Returns Kafka's exact signed broker code when present.
    pub const fn broker_code(self) -> Option<i16> {
        self.broker_code
    }
}

/// Authoritative delivery certainty for one failed `EndTxn`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndDeliveryStatus {
    /// The `EndTxn` request did not cross the transport ownership boundary.
    NotSent,
    /// Kafka may have observed the `EndTxn` request.
    PossiblySent,
}

/// Sole named observer for one accepted explicit commit or abort.
#[must_use = "dropping abandons observation without cancelling the accepted transaction end"]
pub struct TransactionEndObserver {
    inner: CompletionObserver<TransactionLifecycleTerminal>,
    intent: TransactionEndIntent,
    _lifetime: Arc<dyn Send + Sync>,
}

impl TransactionEndObserver {
    pub(super) const fn new(
        inner: CompletionObserver<TransactionLifecycleTerminal>,
        intent: TransactionEndIntent,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner,
            intent,
            _lifetime: lifetime,
        }
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(self) -> Result<TransactionEndOutcome, TransactionEndObserverError> {
        self.inner
            .wait()
            .map(|terminal| translate_terminal(terminal, self.intent))
            .map_err(observer_error)
    }
}

impl Future for TransactionEndObserver {
    type Output = Result<TransactionEndOutcome, TransactionEndObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(terminal)) => Poll::Ready(Ok(translate_terminal(terminal, this.intent))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for TransactionEndObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionEndObserver")
            .finish_non_exhaustive()
    }
}

/// Failure to observe one accepted transaction end terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The bounded completion generation is no longer live.
    Stale,
}

impl fmt::Display for TransactionEndObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "transaction end was already observed",
            Self::Stale => "transaction end observer is stale",
        })
    }
}

impl std::error::Error for TransactionEndObserverError {}
