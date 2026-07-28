//! Stable SCRAM mechanism and non-secret credential fact tests.

use super::{ScramCredentialInfo, ScramMechanism};

#[test]
fn mechanism_preserves_known_and_future_signed_codes() {
    assert_eq!(ScramMechanism::SHA_256.code(), 1);
    assert_eq!(ScramMechanism::SHA_512.code(), 2);
    assert_eq!(ScramMechanism::from_code(-91).code(), -91);
    assert_eq!(ScramMechanism::from_code(73).code(), 73);
}

#[test]
fn credential_info_exposes_only_mechanism_and_iterations() {
    let info = ScramCredentialInfo::new(ScramMechanism::SHA_512, 16_384);

    assert_eq!(info.mechanism(), ScramMechanism::SHA_512);
    assert_eq!(info.iterations(), 16_384);
    assert!(!format!("{info:?}").contains("password"));
}
