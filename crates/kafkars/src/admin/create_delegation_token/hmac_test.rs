//! Delegation-token HMAC redaction, transfer, and zeroization scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts exact HMAC decoding outcomes"
)]

use super::{DelegationTokenHmac, DelegationTokenHmacError};

const MAX_HMAC_BYTES: usize = 64 * 1024;

#[test]
fn diagnostics_never_expose_secret_bytes() {
    let hmac = DelegationTokenHmac::new(b"hmac-must-not-leak".to_vec());
    let diagnostic = format!("{hmac:?}");

    assert!(diagnostic.contains("redacted"));
    assert!(!diagnostic.contains("hmac-must-not-leak"));
    assert_eq!(hmac.len(), 18);
    assert!(!hmac.is_empty());
}

#[test]
fn explicit_zeroization_and_unique_transfer_clear_the_owner() {
    let mut hmac = DelegationTokenHmac::new(b"secret".to_vec());
    hmac.zeroize_for_test();
    assert!(hmac.is_empty());

    let transferred = DelegationTokenHmac::new(vec![1, 2, 3]).into_bytes();
    assert_eq!(transferred, [1, 2, 3]);
}

#[test]
fn hmac_is_send_and_owns_a_drop_path() {
    fn assert_send<T: Send>() {}

    assert_send::<DelegationTokenHmac>();
    assert!(std::mem::needs_drop::<DelegationTokenHmac>());
}

#[test]
fn durable_bytes_reconstruct_exact_bounded_secret() {
    let hmac = DelegationTokenHmac::from_bytes(vec![7; MAX_HMAC_BYTES])
        .expect("the 64-KiB boundary must remain representable");

    assert_eq!(hmac.len(), MAX_HMAC_BYTES);
    assert!(hmac.as_bytes().iter().all(|byte| *byte == 7));
}

#[test]
fn durable_bytes_reject_empty_and_oversized_values_without_secret_diagnostics() {
    let empty = DelegationTokenHmac::from_bytes(Vec::new())
        .expect_err("an empty token HMAC must be rejected");
    assert_eq!(empty, DelegationTokenHmacError::Empty);

    let oversized = DelegationTokenHmac::from_bytes(vec![9; MAX_HMAC_BYTES + 1])
        .expect_err("an oversized token HMAC must be rejected");
    assert_eq!(
        oversized,
        DelegationTokenHmacError::TooLong {
            actual: MAX_HMAC_BYTES + 1,
            maximum: MAX_HMAC_BYTES,
        }
    );
    let diagnostic = format!("{oversized:?} {oversized}");
    assert!(!diagnostic.contains("[9"));
    assert!(!diagnostic.contains("secret"));
}
