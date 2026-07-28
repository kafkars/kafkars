//! Public SASL mechanism and redaction scenarios.

use super::{Sasl, SaslMechanism};

#[test]
fn constructors_preserve_each_supported_mechanism() {
    assert_eq!(
        Sasl::plain("user", "password").mechanism(),
        SaslMechanism::Plain
    );
    assert_eq!(
        Sasl::scram_sha_256("user", "password").mechanism(),
        SaslMechanism::ScramSha256
    );
    assert_eq!(
        Sasl::scram_sha_512("user", "password").mechanism(),
        SaslMechanism::ScramSha512
    );
}

#[test]
fn credential_diagnostics_are_redacted() {
    let diagnostic = format!(
        "{:?}",
        Sasl::scram_sha_512("private-user", "private-password")
    );

    assert!(diagnostic.contains("ScramSha512"));
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-password"));
}
