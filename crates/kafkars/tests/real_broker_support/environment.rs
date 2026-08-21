//! Shared real-cluster client security and bootstrap environment.

use std::{env, fs, io};

use kafkars::{Client, ClientBuilder, Sasl, Security, Tls};

use super::TestError;

pub(crate) fn client_builder_from_environment(client_id: &str) -> Result<ClientBuilder, TestError> {
    let bootstrap = env::var("KAFKA_CLIENT_TEST_BOOTSTRAP").map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "set KAFKA_CLIENT_TEST_BOOTSTRAP to comma-separated broker endpoints",
        )
    })?;
    let servers = bootstrap
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKA_CLIENT_TEST_BOOTSTRAP contains no broker endpoints",
        )
        .into());
    }

    Ok(Client::builder()
        .bootstrap_servers(servers)
        .client_id(client_id)
        .security(security_from_environment()?))
}

fn security_from_environment() -> Result<Security, TestError> {
    match env::var("KAFKA_CLIENT_TEST_SECURITY").as_deref() {
        Err(env::VarError::NotPresent) | Ok("plaintext") => Ok(Security::plaintext()),
        Ok("tls") => Ok(Security::tls(tls_from_environment()?)),
        Ok("sasl_plaintext") => Ok(Security::sasl_plaintext(sasl_from_environment()?)),
        Ok("sasl_tls") => Ok(Security::sasl_tls(
            tls_from_environment()?,
            sasl_from_environment()?,
        )),
        Ok(_) | Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKA_CLIENT_TEST_SECURITY must be plaintext, tls, sasl_plaintext, or sasl_tls",
        )
        .into()),
    }
}

fn tls_from_environment() -> Result<Tls, TestError> {
    match env::var("KAFKA_CLIENT_TEST_TLS_CA_PEM") {
        Ok(path) => Ok(Tls::custom_roots_pem(fs::read(path)?)),
        Err(env::VarError::NotPresent) => Ok(Tls::system_roots()),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKA_CLIENT_TEST_TLS_CA_PEM must be a valid path",
        )
        .into()),
    }
}

fn sasl_from_environment() -> Result<Sasl, TestError> {
    let username = required_environment("KAFKA_CLIENT_TEST_SASL_USERNAME")?;
    let password = required_environment("KAFKA_CLIENT_TEST_SASL_PASSWORD")?;
    match env::var("KAFKA_CLIENT_TEST_SASL_MECHANISM").as_deref() {
        Ok("plain") => Ok(Sasl::plain(username, password)),
        Ok("scram_sha_256") => Ok(Sasl::scram_sha_256(username, password)),
        Ok("scram_sha_512") => Ok(Sasl::scram_sha_512(username, password)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKA_CLIENT_TEST_SASL_MECHANISM must be plain, scram_sha_256, or scram_sha_512",
        )
        .into()),
    }
}

fn required_environment(name: &str) -> Result<String, io::Error> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("set required smoke environment variable {name}"),
        )
    })
}
