//! Engine-owned terminal values from one ordered `DescribeConfigs` batch.

/// Stable admin delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConfigsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// One deterministic configuration synonym.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigSynonym {
    pub(super) name: String,
    pub(super) value: Option<String>,
    pub(super) source: i8,
}

impl DescribeConfigSynonym {
    /// Consumes this synonym into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, Option<String>, i8) {
        (self.name, self.value, self.source)
    }
}

/// One normalized Kafka configuration entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigEntry {
    pub(super) name: String,
    pub(super) value: Option<String>,
    pub(super) read_only: bool,
    pub(super) source: i8,
    pub(super) sensitive: bool,
    pub(super) synonyms: Vec<DescribeConfigSynonym>,
    pub(super) config_type: Option<i8>,
    pub(super) documentation: Option<String>,
}

impl DescribeConfigEntry {
    /// Returns the configuration name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nullable configuration value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns whether Kafka marks this configuration read-only.
    pub const fn read_only(&self) -> bool {
        self.read_only
    }

    /// Returns Kafka's exact signed configuration source.
    pub const fn source(&self) -> i8 {
        self.source
    }

    /// Returns whether Kafka marks the value sensitive.
    pub const fn sensitive(&self) -> bool {
        self.sensitive
    }

    /// Returns deterministic configuration synonyms.
    pub fn synonyms(&self) -> &[DescribeConfigSynonym] {
        &self.synonyms
    }

    /// Returns the field present in `DescribeConfigs` v3 and newer.
    pub const fn config_type(&self) -> Option<i8> {
        self.config_type
    }

    /// Returns version-present nullable documentation.
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }

    /// Consumes this entry into stable adapter-owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<String>,
        bool,
        i8,
        bool,
        Vec<DescribeConfigSynonym>,
        Option<i8>,
        Option<String>,
    ) {
        (
            self.name,
            self.value,
            self.read_only,
            self.source,
            self.sensitive,
            self.synonyms,
            self.config_type,
            self.documentation,
        )
    }
}

/// Exact broker-declared resource rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigResourceError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeConfigResourceError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns the nullable bounded broker diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether the diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this broker rejection into stable adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One requested resource result in original order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigResourceResult {
    pub(super) resource_type: i8,
    pub(super) resource_name: String,
    pub(super) result: Result<Vec<DescribeConfigEntry>, DescribeConfigResourceError>,
}

impl DescribeConfigResourceResult {
    /// Consumes the ordered result into stable adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        i8,
        String,
        Result<Vec<DescribeConfigEntry>, DescribeConfigResourceError>,
    ) {
        (self.resource_type, self.resource_name, self.result)
    }
}

/// Successful ordered response plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConfigsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) resources: Vec<DescribeConfigResourceResult>,
}

impl DescribeConfigsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Consumes the response into its ordered resource results.
    pub fn into_resources(self) -> Vec<DescribeConfigResourceResult> {
        self.resources
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConfigsFailureKind {
    /// The original deadline elapsed.
    DeadlineElapsed,
    /// The generated request was rejected before driver ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The broker response could not be correlated.
    InvalidResponse,
    /// A valid response exceeded admitted result capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent the requested semantics.
    Compatibility,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeConfigsFailure {
    pub(super) kind: DescribeConfigsFailureKind,
    pub(super) delivery: DescribeConfigsDeliveryStatus,
}

impl DescribeConfigsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeConfigsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeConfigsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeConfigsOutcome {
    /// Ordered broker outcomes and throttle observation.
    Configs(DescribeConfigsBatch),
    /// Whole-operation failure.
    Failed(DescribeConfigsFailure),
}
