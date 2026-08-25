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
#[derive(PartialEq, Eq)]
pub struct ByteBudget {
    accounting: bytebudget::ByteBudget,
}

impl fmt::Debug for ByteBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteBudget")
            .field("limit", &self.limit())
            .field("used", &self.used())
            .finish()
    }
}

impl ByteBudget {
    /// Creates an empty budget with the supplied hard limit.
    pub const fn new(limit: ByteCount) -> Self {
        Self {
            accounting: bytebudget::ByteBudget::new(bytebudget::ByteCount::new(limit.get())),
        }
    }

    /// Returns the hard limit.
    pub const fn limit(&self) -> ByteCount {
        ByteCount::new(self.accounting.limit().get())
    }

    /// Returns bytes currently retained.
    pub const fn used(&self) -> ByteCount {
        ByteCount::new(self.accounting.used().get())
    }

    /// Returns bytes still available.
    pub const fn available(&self) -> ByteCount {
        ByteCount::new(self.accounting.available().get())
    }

    /// Reserves bytes atomically or leaves the budget unchanged.
    pub fn try_reserve(&mut self, bytes: ByteCount) -> Result<(), CapacityError> {
        if self.used().checked_add(bytes).is_none() {
            return Err(CapacityError::Overflow);
        }
        self.accounting
            .try_reserve(bytebudget::ByteCount::new(bytes.get()))
            .map_err(|_| CapacityError::Exhausted)
    }

    /// Releases bytes previously reserved.
    pub fn release(&mut self, bytes: ByteCount) -> Result<(), CapacityError> {
        let plan = self.plan_release(bytes)?;
        self.commit_release(plan);
        Ok(())
    }

    pub(crate) fn plan_release(&self, bytes: ByteCount) -> Result<ByteReleasePlan, CapacityError> {
        let expected_used = self.used();
        let Some(next_used) = expected_used.checked_sub(bytes) else {
            return Err(CapacityError::OverRelease);
        };
        let mut next_accounting = bytebudget::ByteBudget::new(self.accounting.limit());
        if next_accounting
            .try_reserve(bytebudget::ByteCount::new(next_used.get()))
            .is_err()
        {
            return Err(CapacityError::Overflow);
        }
        Ok(ByteReleasePlan {
            expected_used,
            next_accounting,
        })
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "the preflight plan is a linear commit capability"
    )]
    pub(crate) fn commit_release(&mut self, plan: ByteReleasePlan) {
        let ByteReleasePlan {
            expected_used,
            next_accounting,
        } = plan;
        debug_assert_eq!(self.used(), expected_used);
        self.accounting = next_accounting;
    }
}

/// Preflighted retained-byte release consumed by its mutation owner.
#[derive(Debug)]
pub(crate) struct ByteReleasePlan {
    expected_used: ByteCount,
    next_accounting: bytebudget::ByteBudget,
}
