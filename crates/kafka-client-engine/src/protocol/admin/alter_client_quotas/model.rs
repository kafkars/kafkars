//! Borrowed alteration input and generated-free normalized response facts.

/// One canonical component of a client-quota entity identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AlterClientQuotaEntityComponentRef<'a> {
    entity_type: &'a str,
    entity_name: Option<&'a str>,
}

impl<'a> AlterClientQuotaEntityComponentRef<'a> {
    pub(crate) const fn new(entity_type: &'a str, entity_name: Option<&'a str>) -> Self {
        Self {
            entity_type,
            entity_name,
        }
    }

    pub(crate) const fn entity_type(self) -> &'a str {
        self.entity_type
    }

    pub(crate) const fn entity_name(self) -> Option<&'a str> {
        self.entity_name
    }
}

/// The wire-level meaning of one client-quota alteration.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum AlterClientQuotaOperationKindRef {
    Set(f64),
    Remove,
}

/// One borrowed quota key and the operation applied to it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AlterClientQuotaOperationRef<'a> {
    key: &'a str,
    kind: AlterClientQuotaOperationKindRef,
}

impl<'a> AlterClientQuotaOperationRef<'a> {
    pub(crate) const fn new(key: &'a str, kind: AlterClientQuotaOperationKindRef) -> Self {
        Self { key, kind }
    }

    pub(crate) const fn key(self) -> &'a str {
        self.key
    }

    pub(crate) const fn kind(self) -> AlterClientQuotaOperationKindRef {
        self.kind
    }
}

/// One caller-ordered entity and its nonempty quota-operation set.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AlterClientQuotaAlterationRef<'a> {
    entity: &'a [AlterClientQuotaEntityComponentRef<'a>],
    operations: &'a [AlterClientQuotaOperationRef<'a>],
}

impl<'a> AlterClientQuotaAlterationRef<'a> {
    pub(crate) const fn new(
        entity: &'a [AlterClientQuotaEntityComponentRef<'a>],
        operations: &'a [AlterClientQuotaOperationRef<'a>],
    ) -> Self {
        Self { entity, operations }
    }

    pub(crate) const fn entity(self) -> &'a [AlterClientQuotaEntityComponentRef<'a>] {
        self.entity
    }

    pub(crate) const fn operations(self) -> &'a [AlterClientQuotaOperationRef<'a>] {
        self.operations
    }
}

/// One complete borrowed API-key 49 input.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AlterClientQuotasRequestRef<'a> {
    alterations: &'a [AlterClientQuotaAlterationRef<'a>],
    validate_only: bool,
}

impl<'a> AlterClientQuotasRequestRef<'a> {
    pub(crate) const fn new(
        alterations: &'a [AlterClientQuotaAlterationRef<'a>],
        validate_only: bool,
    ) -> Self {
        Self {
            alterations,
            validate_only,
        }
    }

    pub(crate) const fn alterations(self) -> &'a [AlterClientQuotaAlterationRef<'a>] {
        self.alterations
    }

    pub(crate) const fn validate_only(self) -> bool {
        self.validate_only
    }
}

/// One owned canonical entity component in a validated response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterClientQuotaEntityComponent {
    pub(super) entity_type: String,
    pub(super) entity_name: Option<String>,
}

impl NormalizedAlterClientQuotaEntityComponent {
    pub(crate) fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }
}

/// One caller-ordered entity outcome with exact broker facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterClientQuotaOutcome {
    pub(super) entity: Vec<NormalizedAlterClientQuotaEntityComponent>,
    pub(super) error_code: i16,
    pub(super) error_message: Option<String>,
    pub(super) error_message_truncated: bool,
}

impl NormalizedAlterClientQuotaOutcome {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<NormalizedAlterClientQuotaEntityComponent>,
        i16,
        Option<String>,
        bool,
    ) {
        (
            self.entity,
            self.error_code,
            self.error_message,
            self.error_message_truncated,
        )
    }
}

/// One validated API-key 49 response restored to caller alteration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterClientQuotasResponse {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<NormalizedAlterClientQuotaOutcome>,
    pub(super) retained_bytes: usize,
}

impl NormalizedAlterClientQuotasResponse {
    pub(crate) fn into_parts(self) -> (u32, Vec<NormalizedAlterClientQuotaOutcome>, usize) {
        (self.throttle_time_ms, self.outcomes, self.retained_bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CanonicalEntityComponentRef<'a> {
    pub(super) entity_type: &'a str,
    pub(super) entity_name: Option<&'a str>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CanonicalEntityRef<'a> {
    pub(super) components: Vec<CanonicalEntityComponentRef<'a>>,
}
