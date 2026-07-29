//! Explicit bounded owner selection for Admin `DescribeDelegationToken`.

use core::fmt;
use std::collections::BTreeSet;

use super::super::DelegationTokenPrincipal;

/// Maximum explicit owners retained by one token-description request.
pub const DESCRIBE_DELEGATION_TOKENS_MAX_OWNERS: usize = 4 * 1024;
/// Maximum aggregate owner text retained by one token-description request.
pub const DESCRIBE_DELEGATION_TOKENS_MAX_REQUEST_TEXT_BYTES: usize = 256 * 1024;

/// Explicit token-selection mode; an empty owner batch never means all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensSelection {
    /// Every token visible to the authenticated principal.
    All,
    /// Tokens owned by this nonempty unique caller-ordered owner set.
    Owners(Vec<DelegationTokenPrincipal>),
}

/// Validated deterministic intent for one token-description operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensPlan {
    selection: DescribeDelegationTokensSelection,
}

impl DescribeDelegationTokensPlan {
    /// Creates an explicit all-token query.
    pub const fn all() -> Self {
        Self {
            selection: DescribeDelegationTokensSelection::All,
        }
    }

    /// Validates a nonempty caller-ordered set of unique owners.
    pub fn for_owners(
        owners: Vec<DelegationTokenPrincipal>,
    ) -> Result<Self, DescribeDelegationTokensPlanError> {
        if owners.is_empty() {
            return Err(DescribeDelegationTokensPlanError::EmptyOwners);
        }
        if owners.len() > DESCRIBE_DELEGATION_TOKENS_MAX_OWNERS {
            return Err(DescribeDelegationTokensPlanError::TooManyOwners);
        }
        let mut identities = BTreeSet::new();
        let mut retained_text = 0usize;
        for owner in &owners {
            if !identities.insert(owner) {
                return Err(DescribeDelegationTokensPlanError::DuplicateOwner);
            }
            retained_text = retained_text
                .checked_add(owner.principal_type().len())
                .and_then(|bytes| bytes.checked_add(owner.principal_name().len()))
                .ok_or(DescribeDelegationTokensPlanError::RequestTextBytesExceeded)?;
            if retained_text > DESCRIBE_DELEGATION_TOKENS_MAX_REQUEST_TEXT_BYTES {
                return Err(DescribeDelegationTokensPlanError::RequestTextBytesExceeded);
            }
        }
        Ok(Self {
            selection: DescribeDelegationTokensSelection::Owners(owners),
        })
    }

    /// Returns the exact explicit query selection.
    pub const fn selection(&self) -> &DescribeDelegationTokensSelection {
        &self.selection
    }
}

/// Invalid deterministic token-description intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensPlanError {
    /// Empty owner selection is ambiguous with an all-token query.
    EmptyOwners,
    /// One operation exceeded its explicit owner-count bound.
    TooManyOwners,
    /// The exact same owner appeared more than once.
    DuplicateOwner,
    /// Aggregate owner text exceeded the deterministic request bound.
    RequestTextBytesExceeded,
}

impl fmt::Display for DescribeDelegationTokensPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeDelegationTokens plan: {self:?}")
    }
}

impl std::error::Error for DescribeDelegationTokensPlanError {}
