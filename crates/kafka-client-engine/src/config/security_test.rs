//! Engine-owned security vocabulary and redacted diagnostic scenarios.

use super::{EngineSasl, EngineSaslMechanism, EngineSecurity, EngineTls};

#[test]
fn engine_security_diagnostics_are_secret_free() {
    let sasl = EngineSasl::scram_sha_512("private-user", "private-password");
    assert_eq!(sasl.mechanism(), EngineSaslMechanism::ScramSha512);
    let diagnostic = format!(
        "{:?}",
        EngineSecurity::sasl_tls(EngineTls::system_roots(), sasl)
    );

    assert!(diagnostic.contains("ScramSha512"));
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-password"));
}

#[test]
fn custom_root_diagnostics_are_bounded_and_redacted() {
    let tls = EngineTls::custom_roots_pem(b"private-custom-root".to_vec());
    assert_eq!(
        tls.custom_roots_pem_bytes(),
        Some(b"private-custom-root".as_slice())
    );
    let diagnostic = format!("{tls:?}");

    assert!(diagnostic.contains("CustomRootsPem"));
    assert!(!diagnostic.contains("private-custom-root"));
}
