//! Scenarios for facade-owned engine startup and child-handle retention.

use kafka_client_engine::{
    ConsumerReadIsolation as EngineReadIsolation, EngineSaslMechanism, EngineSecurity,
};

use super::{
    client::{ClientEngine, engine_security},
    consumer_configuration::engine_read_isolation,
};
use crate::{
    ConsumerFetchConfig, ConsumerLimits, ErrorKind, Sasl, Security, Tls,
    producer::{Compression, ProducerLimits},
};

#[test]
fn client_bridge_retains_validated_endpoints_and_builds_a_producer() {
    let result = ClientEngine::start_with_consumer_fetch(
        vec!["127.0.0.1:1".to_owned()],
        None,
        Security::plaintext(),
        Compression::None,
        ProducerLimits::default(),
        None,
        ConsumerFetchConfig::default(),
        ConsumerLimits::default(),
    );
    let Ok(client) = result else {
        panic!("valid local engine configuration should start")
    };

    assert_eq!(client.bootstrap_servers(), &["127.0.0.1:1".to_owned()]);
    let _producer = client.producer();
}

#[test]
fn facade_security_maps_exhaustively_to_engine_configuration() {
    let modes = [
        Security::plaintext(),
        Security::tls(Tls::system_roots()),
        Security::sasl_plaintext(Sasl::plain("plain-user", "plain-password")),
        Security::sasl_tls(
            Tls::system_roots(),
            Sasl::scram_sha_256("sha-user", "sha-password"),
        ),
        Security::sasl_tls(
            Tls::system_roots(),
            Sasl::scram_sha_512("sha-user", "sha-password"),
        ),
    ];

    assert!(matches!(
        engine_security(&modes[0]),
        EngineSecurity::Plaintext
    ));
    assert!(matches!(engine_security(&modes[1]), EngineSecurity::Tls(_)));
    for (mode, mechanism) in [
        (&modes[2], EngineSaslMechanism::Plain),
        (&modes[3], EngineSaslMechanism::ScramSha256),
        (&modes[4], EngineSaslMechanism::ScramSha512),
    ] {
        let mapped = engine_security(mode);
        let sasl = match mapped {
            EngineSecurity::SaslPlaintext(sasl) | EngineSecurity::SaslTls { sasl, .. } => sasl,
            other => panic!("SASL mode expected, got {other:?}"),
        };
        assert_eq!(sasl.mechanism(), mechanism);
    }
}

#[test]
fn custom_tls_root_bundle_reaches_engine_configuration_without_diagnostics() {
    let engine = engine_security(&Security::tls(Tls::custom_roots_pem(
        b"private-custom-root".to_vec(),
    )));
    let diagnostic = format!("{engine:?}");

    assert!(diagnostic.contains("CustomRootsPem"));
    assert!(!diagnostic.contains("private-custom-root"));
}

#[test]
fn invalid_custom_tls_roots_are_configuration_errors_without_material_disclosure() {
    let certificate_material = "private-invalid-certificate-material";
    let result = ClientEngine::start_with_consumer_fetch(
        vec!["127.0.0.1:1".to_owned()],
        None,
        Security::tls(Tls::custom_roots_pem(
            certificate_material.as_bytes().to_vec(),
        )),
        Compression::None,
        ProducerLimits::default(),
        None,
        ConsumerFetchConfig::default(),
        ConsumerLimits::default(),
    );
    let Err(error) = result else {
        panic!("invalid custom TLS roots must be rejected");
    };
    let diagnostic = error.to_string();

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(!diagnostic.contains(certificate_material));
}

#[test]
fn rejected_security_diagnostics_do_not_expose_credentials() {
    let password = "private\0password";
    let result = ClientEngine::start_with_consumer_fetch(
        vec!["127.0.0.1:1".to_owned()],
        None,
        Security::sasl_plaintext(Sasl::plain("private-user", password)),
        Compression::None,
        ProducerLimits::default(),
        None,
        ConsumerFetchConfig::default(),
        ConsumerLimits::default(),
    );
    let Err(error) = result else {
        panic!("NUL-containing SASL credentials must be rejected");
    };
    let diagnostic = error.to_string();

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains(password));
}

#[test]
fn facade_read_isolation_maps_exhaustively_to_engine_configuration() {
    for (public, engine) in [
        (
            crate::ReadIsolation::ReadUncommitted,
            EngineReadIsolation::ReadUncommitted,
        ),
        (
            crate::ReadIsolation::ReadCommitted,
            EngineReadIsolation::ReadCommitted,
        ),
    ] {
        assert_eq!(engine_read_isolation(public), engine);
    }
}
