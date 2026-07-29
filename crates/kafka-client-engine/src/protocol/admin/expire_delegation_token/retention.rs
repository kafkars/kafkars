//! Checked one-MiB request and scalar-response envelope charges.

use core::mem::size_of;

use kafka_wire::ExpireDelegationTokenRequest;

use super::{
    ExpireDelegationTokenRequestRef, NormalizedExpireDelegationTokenResponse,
    PreparedExpireDelegationTokenRequest,
};

/// Absolute envelope for one retained expiration request or terminal.
pub(crate) const EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES: usize = 1_024 * 1_024;
pub(super) const MAX_HMAC_BYTES: usize = EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES;

/// Conservatively covers canonical bytes, generated adaptation, and encoded frame.
pub(super) fn request_peak_charge(source: ExpireDelegationTokenRequestRef<'_>) -> Option<usize> {
    let secret_copies = source.hmac().len().checked_mul(3)?;
    size_of::<PreparedExpireDelegationTokenRequest>()
        .checked_add(size_of::<ExpireDelegationTokenRequest>())?
        .checked_add(secret_copies)?
        .checked_add(64)
}

pub(super) const fn response_charge() -> usize {
    size_of::<NormalizedExpireDelegationTokenResponse>()
}
