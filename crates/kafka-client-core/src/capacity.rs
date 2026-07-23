//! Explicit retained-byte budget used by bounded admission policy.

use core::fmt;

use crate::ByteCount;

/// Rejected byte-budget transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityError {
    /// Reserving the requested bytes would exceed the configured limit.
    Exhausted,
    /// Arithmetic overflow prevented a trustworthy capacity decision.
    Overflow,
    /// A release exceeded the bytes currently retained.
    OverRelease,
}

impl fmt::Display for CapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted => formatter.write_str("retained-byte capacity exhausted"),
            Self::Overflow => formatter.write_str("retained-byte accounting overflow"),
            Self::OverRelease => formatter.write_str("released more bytes than retained"),
        }
    }
}

impl std::error::Error for CapacityError {}

/// Deterministic retained-byte budget.
#[derive(Debug, PartialEq, Eq)]
pub struct ByteBudget {
    limit: ByteCount,
    used: ByteCount,
}

impl ByteBudget {
    /// Creates an empty budget with the supplied hard limit.
    pub const fn new(limit: ByteCount) -> Self {
        Self {
            limit,
            used: ByteCount::new(0),
        }
    }

    /// Returns the hard limit.
    pub const fn limit(&self) -> ByteCount {
        self.limit
    }

    /// Returns bytes currently retained.
    pub const fn used(&self) -> ByteCount {
        self.used
    }

    /// Returns bytes still available.
    pub const fn available(&self) -> ByteCount {
        ByteCount::new(self.limit.get() - self.used.get())
    }

    /// Reserves bytes atomically or leaves the budget unchanged.
    pub fn try_reserve(&mut self, bytes: ByteCount) -> Result<(), CapacityError> {
        let Some(next) = self.used.checked_add(bytes) else {
            return Err(CapacityError::Overflow);
        };
        if next > self.limit {
            return Err(CapacityError::Exhausted);
        }
        self.used = next;
        Ok(())
    }

    /// Releases bytes previously reserved.
    pub fn release(&mut self, bytes: ByteCount) -> Result<(), CapacityError> {
        let plan = self.plan_release(bytes)?;
        self.commit_release(plan);
        Ok(())
    }

    pub(crate) fn plan_release(&self, bytes: ByteCount) -> Result<ByteReleasePlan, CapacityError> {
        let Some(next_used) = self.used.checked_sub(bytes) else {
            return Err(CapacityError::OverRelease);
        };
        Ok(ByteReleasePlan { next_used })
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "the preflight plan is a linear commit capability"
    )]
    pub(crate) fn commit_release(&mut self, plan: ByteReleasePlan) {
        let ByteReleasePlan { next_used } = plan;
        self.used = next_used;
    }
}

/// Preflighted retained-byte release consumed by its mutation owner.
#[derive(Debug)]
pub(crate) struct ByteReleasePlan {
    next_used: ByteCount,
}
