//! Stable engine terminal values for Admin `DescribeClientQuotas`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// One canonical component identifying a quota entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotaEntityComponent {
    pub(super) entity_type: String,
    pub(super) entity_name: Option<String>,
}

impl DescribeClientQuotaEntityComponent {
    /// Returns the quota entity type.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the explicit entity name, or `None` for Kafka's default entity.
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Consumes this component into stable scalar parts.
    pub fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }
}

/// One quota configuration key and finite value.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotaValue {
    pub(super) key: String,
    pub(super) value: f64,
}

impl DescribeClientQuotaValue {
    /// Returns the quota configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns Kafka's finite quota value.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Consumes this quota into stable scalar parts.
    pub fn into_parts(self) -> (String, f64) {
        (self.key, self.value)
    }
}

/// One canonical quota entity and its canonically ordered values.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotaEntity {
    pub(super) components: Vec<DescribeClientQuotaEntityComponent>,
    pub(super) values: Vec<DescribeClientQuotaValue>,
}

impl DescribeClientQuotaEntity {
    /// Returns entity components ordered by entity-type UTF-8 bytes.
    pub fn components(&self) -> &[DescribeClientQuotaEntityComponent] {
        &self.components
    }

    /// Returns quota values ordered by configuration-key UTF-8 bytes.
    pub fn values(&self) -> &[DescribeClientQuotaValue] {
        &self.values
    }

    /// Consumes this entity into stable components and values.
    pub fn into_parts(
        self,
    ) -> (
        Vec<DescribeClientQuotaEntityComponent>,
        Vec<DescribeClientQuotaValue>,
    ) {
        (self.components, self.values)
    }
}

/// Deterministically ordered quota entities plus Kafka's throttle observation.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotasBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) entities: Vec<DescribeClientQuotaEntity>,
}

impl DescribeClientQuotasBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns entities in canonical entity-component order.
    pub fn entities(&self) -> &[DescribeClientQuotaEntity] {
        &self.entities
    }

    /// Consumes throttle and deterministically ordered quota entities.
    pub fn into_parts(self) -> (u32, Vec<DescribeClientQuotaEntity>) {
        (self.throttle_time_ms, self.entities)
    }
}

/// Exact Kafka top-level rejection and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeClientQuotasBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the rejection into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the query with an exact top-level error.
    Broker(DescribeClientQuotasBrokerError),
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasFailure {
    pub(super) kind: DescribeClientQuotasFailureKind,
    pub(super) delivery: DescribeClientQuotasDeliveryStatus,
}

impl DescribeClientQuotasFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> &DescribeClientQuotasFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> DescribeClientQuotasDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, PartialEq)]
pub enum DescribeClientQuotasOutcome {
    /// Kafka returned zero or more deterministically ordered quota entities.
    Described(DescribeClientQuotasBatch),
    /// The operation failed outside a valid quota-entity set.
    Failed(DescribeClientQuotasFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeClientQuotasObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeClientQuotas result was already observed",
            Self::Stale => "Admin DescribeClientQuotas observer is stale",
        })
    }
}

impl std::error::Error for DescribeClientQuotasObserverError {}
