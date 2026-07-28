//! Validated security transfer into practical embedded driver construction.

use crate::{EngineConfig, EngineSasl, EngineSecurity, EngineTls, driver::DriverOwner};

use super::security::{EngineSecurityError, ValidatedSecurity, validate};

const CUSTOM_ROOT: &[u8] = include_bytes!("fixtures/custom-root.pem");

#[test]
fn all_validated_security_modes_build_an_embedded_driver() {
    let modes = vec![
        EngineSecurity::plaintext(),
        EngineSecurity::tls(EngineTls::custom_roots_pem(CUSTOM_ROOT.to_vec())),
        EngineSecurity::sasl_plaintext(EngineSasl::plain("plain-user", "plain-password")),
        EngineSecurity::sasl_tls(
            EngineTls::custom_roots_pem(CUSTOM_ROOT.to_vec()),
            EngineSasl::scram_sha_256("sha-user", "sha-password"),
        ),
        EngineSecurity::sasl_tls(
            EngineTls::custom_roots_pem(CUSTOM_ROOT.to_vec()),
            EngineSasl::scram_sha_512("sha-user", "sha-password"),
        ),
        EngineSecurity::sasl_tls(
            EngineTls::custom_roots_pem(CUSTOM_ROOT.to_vec()),
            EngineSasl::plain("plain-user", "plain-password"),
        ),
    ];

    for security in modes {
        let config = EngineConfig::new(vec!["localhost:9092".to_owned()]).with_security(security);
        let validated = validate(config.security())
            .unwrap_or_else(|error| panic!("validate security: {error}"));
        let owner = DriverOwner::build_with_security(&config, validated)
            .unwrap_or_else(|error| panic!("build embedded driver: {error}"));

        drop(owner);
    }
}

#[test]
fn custom_root_bundles_are_bounded_and_strictly_parsed() {
    let valid = validate(&EngineSecurity::tls(EngineTls::custom_roots_pem(
        CUSTOM_ROOT.to_vec(),
    )));
    assert!(matches!(valid, Ok(ValidatedSecurity::Tls(_))));
    assert!(matches!(
        validate(&EngineSecurity::tls(
            EngineTls::custom_roots_pem(Vec::new())
        )),
        Err(EngineSecurityError::EmptyCustomRoots)
    ));
    assert!(matches!(
        validate(&EngineSecurity::tls(EngineTls::custom_roots_pem(
            b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----".to_vec()
        ))),
        Err(EngineSecurityError::InvalidCustomRoot)
    ));
    assert!(matches!(
        validate(&EngineSecurity::tls(EngineTls::custom_roots_pem(
            vec![b'x'; 4 * 1024 * 1024 + 1]
        ))),
        Err(EngineSecurityError::CustomRootsTooLarge)
    ));
}

#[test]
fn credentials_are_bounded_and_driver_validation_remains_authoritative() {
    let oversized = "x".repeat(32 * 1024 + 1);
    assert!(matches!(
        validate(&EngineSecurity::sasl_plaintext(EngineSasl::plain(
            oversized, "password"
        ))),
        Err(EngineSecurityError::UsernameTooLong)
    ));
    assert!(matches!(
        validate(&EngineSecurity::sasl_plaintext(EngineSasl::plain(
            "user", "p\0ass"
        ))),
        Err(EngineSecurityError::Sasl(
            kafka_driver::SaslConfigError::PasswordContainsNul
        ))
    ));
}

#[test]
fn validated_security_diagnostics_reuse_driver_redaction() {
    let validated = validate(&EngineSecurity::sasl_plaintext(EngineSasl::plain(
        "private-user",
        "private-password",
    )))
    .unwrap_or_else(|error| panic!("validate PLAIN security: {error}"));
    let ValidatedSecurity::SaslPlaintext(_sasl) = &validated else {
        panic!("SASL/PLAIN driver value expected");
    };
    let diagnostic = format!("{validated:?}");

    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-password"));
}
