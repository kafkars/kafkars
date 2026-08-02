//! Stable semantic errors exposed by the curated facade.

use core::fmt;

/// Certainty attached to a failed network operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// The operation did not cross the transport ownership boundary.
    NotSent,
    /// The operation may have reached Kafka and a blind retry may duplicate it.
    PossiblySent,
}

/// Stable guidance for an application considering a new operation after failure.
///
/// This advice describes duplicate-delivery safety, not whether another
/// attempt is guaranteed to succeed. The client never turns this observation
/// into a hidden retry outside the operation's configured retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAdvice {
    /// Available facts do not justify another application-level attempt.
    DoNotRetry,
    /// A new application-level attempt cannot duplicate this operation.
    RetrySafe,
    /// The failure is transient, but a new attempt may duplicate the operation.
    RetryMayDuplicate,
}

/// Stable top-level category for a client failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Local configuration is incomplete or contradictory.
    Configuration,
    /// A bounded local resource rejected admission.
    Backpressure,
    /// Authentication or authorization rejected the operation.
    Access,
    /// Kafka returned a broker failure without a narrower stable category.
    Broker,
    /// The requested operation is incompatible with the broker.
    Compatibility,
    /// Kafka fenced the producer or transaction identity.
    Fenced,
    /// Kafka rejected record or batch content.
    InvalidRecord,
    /// Cluster metadata or leadership could not route the operation.
    Routing,
    /// The connection failed while the operation was active.
    Transport,
    /// The operation's absolute deadline elapsed.
    Timeout,
    /// Explicit cancellation completed before transport ownership.
    Cancelled,
    /// The requested operation conflicts with the handle lifecycle.
    State,
    /// The implementation violated an internal contract.
    Internal,
}

/// Extensible client error shared by producer, consumer, admin, and transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaError {
    kind: ErrorKind,
    message: String,
    delivery_status: Option<DeliveryStatus>,
    broker_code: Option<i16>,
    internal_topic: Option<bool>,
    diagnostic_truncated: bool,
    transaction_abort_required: bool,
    retry_advice: RetryAdvice,
    fatal: bool,
}

impl KafkaError {
    /// Creates a semantic client error.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            delivery_status: None,
            broker_code: None,
            internal_topic: None,
            diagnostic_truncated: false,
            transaction_abort_required: false,
            retry_advice: RetryAdvice::DoNotRetry,
            fatal: false,
        }
    }

    /// Attaches operation delivery certainty.
    pub fn with_delivery_status(mut self, status: DeliveryStatus) -> Self {
        self.delivery_status = Some(status);
        self
    }

    pub(crate) fn with_broker_code(mut self, broker_code: Option<i16>) -> Self {
        self.broker_code = broker_code;
        self
    }

    pub(crate) const fn with_internal_topic(mut self, internal: bool) -> Self {
        self.internal_topic = Some(internal);
        self
    }

    pub(crate) const fn with_diagnostic_truncated(mut self, truncated: bool) -> Self {
        self.diagnostic_truncated = truncated;
        self
    }

    pub(crate) const fn with_transaction_abort_required(mut self) -> Self {
        self.transaction_abort_required = true;
        self
    }

    pub(crate) const fn with_safe_retry(mut self) -> Self {
        if matches!(self.delivery_status, Some(DeliveryStatus::PossiblySent)) {
            return self;
        }
        self.retry_advice = RetryAdvice::RetrySafe;
        self
    }

    pub(crate) const fn with_duplicate_risk(mut self) -> Self {
        if !matches!(self.delivery_status, Some(DeliveryStatus::PossiblySent)) {
            return self;
        }
        self.retry_advice = RetryAdvice::RetryMayDuplicate;
        self
    }

    pub(crate) const fn with_fatal_disposition(mut self) -> Self {
        self.retry_advice = RetryAdvice::DoNotRetry;
        self.fatal = true;
        self
    }

    /// Returns the stable error category.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns operation delivery certainty when relevant.
    pub const fn delivery_status(&self) -> Option<DeliveryStatus> {
        self.delivery_status
    }

    /// Returns Kafka's exact protocol error code when supplied by a broker.
    pub const fn broker_code(&self) -> Option<i16> {
        self.broker_code
    }

    /// Returns whether the exact failure facts describe a transient condition.
    ///
    /// This does not imply that an application-level retry is duplicate-safe;
    /// inspect [`Self::retry_advice`] before creating a new operation.
    pub const fn is_retriable(&self) -> bool {
        matches!(
            self.retry_advice,
            RetryAdvice::RetrySafe | RetryAdvice::RetryMayDuplicate
        )
    }

    /// Returns whether the originating operation owner cannot safely continue.
    pub const fn is_fatal(&self) -> bool {
        self.fatal
    }

    /// Returns stable application-level retry guidance.
    pub const fn retry_advice(&self) -> RetryAdvice {
        self.retry_advice
    }

    /// Returns Kafka's internal-topic marker for a topic-scoped error.
    pub const fn is_internal_topic(&self) -> Option<bool> {
        self.internal_topic
    }

    /// Returns whether a broker diagnostic was shortened to a bounded prefix.
    pub const fn diagnostic_truncated(&self) -> bool {
        self.diagnostic_truncated
    }

    /// Returns whether the active transaction must now be aborted.
    pub const fn requires_transaction_abort(&self) -> bool {
        self.transaction_abort_required
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for KafkaError {}
