//! Borrowed quota-filter input and generated-free normalized response facts.

/// Kafka's three supported client-quota component match modes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotaMatchRef<'a> {
    Exact(&'a str),
    Default,
    AnySpecified,
}

/// One borrowed component of a client-quota entity filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeClientQuotaFilterComponentRef<'a> {
    entity_type: &'a str,
    match_: DescribeClientQuotaMatchRef<'a>,
}

impl<'a> DescribeClientQuotaFilterComponentRef<'a> {
    pub(crate) const fn new(entity_type: &'a str, match_: DescribeClientQuotaMatchRef<'a>) -> Self {
        Self {
            entity_type,
            match_,
        }
    }

    pub(crate) const fn entity_type(self) -> &'a str {
        self.entity_type
    }

    pub(crate) const fn match_(self) -> DescribeClientQuotaMatchRef<'a> {
        self.match_
    }
}

/// One complete borrowed filter; an empty component slice selects all entities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeClientQuotasFilterRef<'a> {
    components: &'a [DescribeClientQuotaFilterComponentRef<'a>],
    strict: bool,
}

impl<'a> DescribeClientQuotasFilterRef<'a> {
    pub(crate) const fn new(
        components: &'a [DescribeClientQuotaFilterComponentRef<'a>],
        strict: bool,
    ) -> Self {
        Self { components, strict }
    }

    pub(crate) const fn components(self) -> &'a [DescribeClientQuotaFilterComponentRef<'a>] {
        self.components
    }

    pub(crate) const fn strict(self) -> bool {
        self.strict
    }
}

/// One canonical component of a returned client-quota entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedClientQuotaEntityComponent {
    pub(super) entity_type: String,
    pub(super) entity_name: Option<String>,
}

impl NormalizedClientQuotaEntityComponent {
    pub(crate) fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }
}

/// One canonical quota key and its finite broker-supplied value.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NormalizedClientQuotaValue {
    pub(super) key: String,
    pub(super) value: f64,
}

impl NormalizedClientQuotaValue {
    pub(crate) fn into_parts(self) -> (String, f64) {
        (self.key, self.value)
    }
}

/// One canonical quota entity and its key-ordered values.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NormalizedClientQuotaEntry {
    pub(super) entity: Vec<NormalizedClientQuotaEntityComponent>,
    pub(super) values: Vec<NormalizedClientQuotaValue>,
}

impl NormalizedClientQuotaEntry {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<NormalizedClientQuotaEntityComponent>,
        Vec<NormalizedClientQuotaValue>,
    ) {
        (self.entity, self.values)
    }
}

/// One validated API-key 48 response with exact signed broker facts.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NormalizedDescribeClientQuotasResponse {
    pub(super) throttle_time_ms: u32,
    pub(super) error_code: i16,
    pub(super) error_message: Option<String>,
    pub(super) error_message_truncated: bool,
    pub(super) entries: Vec<NormalizedClientQuotaEntry>,
    pub(super) retained_bytes: usize,
}

impl NormalizedDescribeClientQuotasResponse {
    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        i16,
        Option<String>,
        bool,
        Vec<NormalizedClientQuotaEntry>,
        usize,
    ) {
        (
            self.throttle_time_ms,
            self.error_code,
            self.error_message,
            self.error_message_truncated,
            self.entries,
            self.retained_bytes,
        )
    }
}
