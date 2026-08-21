//! Public TLS trust-selection scenarios.

use super::Tls;

#[test]
fn default_tls_uses_system_roots() {
    assert_eq!(Tls::default(), Tls::system_roots());
}

#[test]
fn custom_root_diagnostics_retain_only_the_bundle_size() {
    let tls = Tls::custom_roots_pem(b"private-certificate-material".to_vec());
    assert_eq!(
        tls.custom_roots_pem_bytes(),
        Some(b"private-certificate-material".as_slice())
    );
    let diagnostic = format!("{tls:?}");

    assert!(diagnostic.contains("CustomRootsPem"));
    assert!(diagnostic.contains("28"));
    assert!(!diagnostic.contains("private-certificate-material"));
}
