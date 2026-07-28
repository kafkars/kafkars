//! Security validation and one-way transfer into the driver builder.

use std::{fmt, sync::Arc};

use kafka_driver::{
    BootstrapSet, Driver, DriverBuilder, SaslConfig, SaslConfigError, TlsClientPolicy,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, pem::PemObject},
};

use crate::{EngineSasl, EngineSaslMechanism, EngineSecurity, EngineTls};

const MAX_CUSTOM_ROOT_BYTES: usize = 4 * 1024 * 1024;
const MAX_SASL_FIELD_BYTES: usize = 32 * 1024;

/// Private validated values transferred once into `kafka-driver`.
#[derive(Debug)]
pub(crate) enum ValidatedSecurity {
    Plaintext,
    Tls(TlsClientPolicy),
    SaslPlaintext(SaslConfig),
    SaslTls {
        tls: TlsClientPolicy,
        sasl: SaslConfig,
    },
}

/// Sanitized security-configuration validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EngineSecurityError {
    UsernameTooLong,
    PasswordTooLong,
    Sasl(SaslConfigError),
    NativeRootsUnavailable,
    EmptyNativeRoots,
    InvalidNativeRoot,
    EmptyCustomRoots,
    CustomRootsTooLarge,
    InvalidCustomRoot,
    TlsProtocolVersions,
}

impl fmt::Display for EngineSecurityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::UsernameTooLong => "SASL username exceeds the configured byte limit",
            Self::PasswordTooLong => "SASL password exceeds the configured byte limit",
            Self::Sasl(_source) => "invalid SASL credentials",
            Self::NativeRootsUnavailable => "platform-native TLS roots could not be loaded",
            Self::EmptyNativeRoots => "platform-native TLS roots are empty",
            Self::InvalidNativeRoot => "a platform-native TLS root is invalid",
            Self::EmptyCustomRoots => "custom TLS root bundle contains no certificates",
            Self::CustomRootsTooLarge => "custom TLS root bundle exceeds the configured byte limit",
            Self::InvalidCustomRoot => "custom TLS root bundle contains an invalid certificate",
            Self::TlsProtocolVersions => "safe TLS protocol versions are unavailable",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for EngineSecurityError {}

pub(crate) fn validate(
    security: &EngineSecurity,
) -> Result<ValidatedSecurity, EngineSecurityError> {
    match security {
        EngineSecurity::Plaintext => Ok(ValidatedSecurity::Plaintext),
        EngineSecurity::Tls(tls) => tls_policy(tls).map(ValidatedSecurity::Tls),
        EngineSecurity::SaslPlaintext(sasl) => {
            validate_sasl(sasl).map(ValidatedSecurity::SaslPlaintext)
        }
        EngineSecurity::SaslTls { tls, sasl } => Ok(ValidatedSecurity::SaslTls {
            tls: tls_policy(tls)?,
            sasl: validate_sasl(sasl)?,
        }),
    }
}

fn validate_sasl(sasl: &EngineSasl) -> Result<SaslConfig, EngineSecurityError> {
    let (username, password) = sasl.credentials();
    if username.len() > MAX_SASL_FIELD_BYTES {
        return Err(EngineSecurityError::UsernameTooLong);
    }
    if password.len() > MAX_SASL_FIELD_BYTES {
        return Err(EngineSecurityError::PasswordTooLong);
    }
    match sasl.mechanism() {
        EngineSaslMechanism::Plain => SaslConfig::plain(username, password),
        EngineSaslMechanism::ScramSha256 => SaslConfig::scram_sha_256(username, password),
        EngineSaslMechanism::ScramSha512 => SaslConfig::scram_sha_512(username, password),
    }
    .map_err(EngineSecurityError::Sasl)
}

fn tls_policy(tls: &EngineTls) -> Result<TlsClientPolicy, EngineSecurityError> {
    match tls.custom_roots_pem_bytes() {
        Some(pem) => custom_roots(pem),
        None => system_roots(),
    }
}

fn system_roots() -> Result<TlsClientPolicy, EngineSecurityError> {
    let loaded = rustls_native_certs::load_native_certs();
    if loaded.certs.is_empty() {
        return if loaded.errors.is_empty() {
            Err(EngineSecurityError::EmptyNativeRoots)
        } else {
            Err(EngineSecurityError::NativeRootsUnavailable)
        };
    }
    let mut roots = RootCertStore::empty();
    for certificate in loaded.certs {
        roots
            .add(certificate)
            .map_err(|_error| EngineSecurityError::InvalidNativeRoot)?;
    }
    client_policy(roots)
}

fn custom_roots(pem: &[u8]) -> Result<TlsClientPolicy, EngineSecurityError> {
    if pem.len() > MAX_CUSTOM_ROOT_BYTES {
        return Err(EngineSecurityError::CustomRootsTooLarge);
    }
    let mut roots = RootCertStore::empty();
    let mut certificates = 0_usize;
    for certificate in CertificateDer::pem_slice_iter(pem) {
        let certificate = certificate.map_err(|_error| EngineSecurityError::InvalidCustomRoot)?;
        roots
            .add(certificate)
            .map_err(|_error| EngineSecurityError::InvalidCustomRoot)?;
        certificates += 1;
    }
    if certificates == 0 {
        return Err(EngineSecurityError::EmptyCustomRoots);
    }
    client_policy(roots)
}

fn client_policy(roots: RootCertStore) -> Result<TlsClientPolicy, EngineSecurityError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_error| EngineSecurityError::TlsProtocolVersions)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsClientPolicy::new(Arc::new(client)))
}

pub(super) fn builder(bootstrap: BootstrapSet, security: ValidatedSecurity) -> DriverBuilder {
    match security {
        ValidatedSecurity::Plaintext => Driver::builder().bootstrap(bootstrap),
        ValidatedSecurity::Tls(tls) => Driver::builder().rustls_bootstrap(bootstrap, tls),
        ValidatedSecurity::SaslPlaintext(sasl) => Driver::builder().bootstrap(bootstrap).sasl(sasl),
        ValidatedSecurity::SaslTls { tls, sasl } => Driver::builder()
            .rustls_bootstrap(bootstrap, tls)
            .sasl(sasl),
    }
}
