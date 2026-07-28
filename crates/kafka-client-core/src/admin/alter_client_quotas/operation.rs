//! Stable set/remove value semantics for one client-quota key.

/// Semantic value change for one quota key.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AlterClientQuotaOperationKind {
    /// Assigns one finite quota value.
    Set(f64),
    /// Removes the explicit quota value.
    Remove,
}

/// One caller-ordered quota-key alteration.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterClientQuotaOperation {
    key: String,
    kind: AlterClientQuotaOperationKind,
}

impl AlterClientQuotaOperation {
    /// Creates one finite-value assignment for enclosing-plan validation.
    pub const fn set(key: String, value: f64) -> Self {
        Self {
            key,
            kind: AlterClientQuotaOperationKind::Set(value),
        }
    }

    /// Creates one explicit-value removal for enclosing-plan validation.
    pub const fn remove(key: String) -> Self {
        Self {
            key,
            kind: AlterClientQuotaOperationKind::Remove,
        }
    }

    /// Returns the stable quota configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact semantic value operation.
    pub const fn kind(&self) -> AlterClientQuotaOperationKind {
        self.kind
    }

    /// Consumes this operation into adapter-owned parts.
    pub fn into_parts(self) -> (String, AlterClientQuotaOperationKind) {
        (self.key, self.kind)
    }
}
