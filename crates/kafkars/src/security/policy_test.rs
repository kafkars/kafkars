//! Public transport and authentication policy scenarios.

use super::{Sasl, Security, Tls};

#[test]
fn constructors_cover_the_supported_transport_matrix() {
    assert_eq!(Security::default(), Security::plaintext());
    let _tls = Security::tls(Tls::system_roots());
    let _plain_sasl = Security::sasl_plaintext(Sasl::plain("plain-user", "plain-password"));
    let _tls_sasl_256 = Security::sasl_tls(
        Tls::system_roots(),
        Sasl::scram_sha_256("sha-user", "sha-password"),
    );
    let _tls_sasl_512 = Security::sasl_tls(
        Tls::system_roots(),
        Sasl::scram_sha_512("sha-user", "sha-password"),
    );
}

#[test]
fn policy_diagnostics_never_contain_credentials() {
    let security = Security::sasl_tls(
        Tls::system_roots(),
        Sasl::scram_sha_512("private-user", "private-password"),
    );
    let diagnostic = format!("{security:?}");

    assert!(diagnostic.contains("ScramSha512"));
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-password"));
}
