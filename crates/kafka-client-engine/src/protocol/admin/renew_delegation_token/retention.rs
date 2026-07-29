//! Checked one-MiB request and scalar-response envelope charges.

use core::mem::size_of;

use kafka_wire::RenewDelegationTokenRequest;

use super::{
    NormalizedRenewDelegationTokenResponse, PreparedRenewDelegationTokenRequest,
    RenewDelegationTokenRequestRef,
};

/// Absolute envelope for one retained renewal request or terminal.
pub(crate) const RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES: usize = 1_024 * 1_024;
pub(super) const MAX_HMAC_BYTES: usize = RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES;

/// Conservatively covers canonical bytes, generated adaptation, and encoded frame.
pub(super) fn request_peak_charge(source: RenewDelegationTokenRequestRef<'_>) -> Option<usize> {
    let secret_copies = source.hmac().len().checked_mul(3)?;
    size_of::<PreparedRenewDelegationTokenRequest>()
        .checked_add(size_of::<RenewDelegationTokenRequest>())?
        .checked_add(secret_copies)?
        .checked_add(64)
}

pub(super) const fn response_charge() -> usize {
    size_of::<NormalizedRenewDelegationTokenResponse>()
}
