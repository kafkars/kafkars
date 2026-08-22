//! Opt-in real-cluster rejection and redaction coverage for configured SASL security.

#[path = "real_broker_support/error.rs"]
mod error;
#[path = "real_broker_support/operation.rs"]
mod operation;

use std::{env, fs, io};

use error::TestError;
use kafkars::{Client, ErrorKind, Sasl, Security, Tls};
use operation::wait_within;

#[test]
#[ignore = "requires SASL smoke environment variables and an authenticated Kafka cluster"]
fn configured_sasl_rejects_wrong_password_without_exposing_credentials() {
    run().unwrap_or_else(|error| panic!("real Kafka SASL rejection smoke failed: {error}"));
}

fn run() -> Result<(), TestError> {
    let bootstrap = required_environment("KAFKA_CLIENT_TEST_BOOTSTRAP")?;
    let username = required_environment("KAFKA_CLIENT_TEST_SASL_USERNAME")?;
    let configured_password = required_environment("KAFKA_CLIENT_TEST_SASL_PASSWORD")?;
    let attempted_password = format!("{configured_password}-incorrect");
    let client = Client::builder()
        .bootstrap_servers(
            bootstrap
                .split(',')
                .map(str::trim)
                .filter(|server| !server.is_empty()),
        )
        .client_id("kafkars-real-sasl-rejection-smoke")
        .security(security_from_environment(
            username,
            attempted_password.clone(),
        )?)
        .build()?;

    let readiness = wait_within(client.ready(), "wrong-password SASL readiness");
    let shutdown = wait_within(client.shutdown(), "wrong-password SASL client shutdown");
    let ready_error = readiness?
        .err()
        .ok_or_else(|| io::Error::other("wrong SASL password unexpectedly authenticated"))?;
    shutdown??;
    if ready_error.kind() != ErrorKind::Access {
        return Err(io::Error::other(format!(
            "wrong SASL password returned unexpected public error kind {:?}",
            ready_error.kind()
        ))
        .into());
    }
    let public_error = format!("{ready_error:?} {ready_error}");
    if public_error.contains(&configured_password) || public_error.contains(&attempted_password) {
        return Err(io::Error::other("SASL authentication error exposed password material").into());
    }
    Ok(())
}

fn security_from_environment(username: String, password: String) -> Result<Security, TestError> {
    let sasl = match env::var("KAFKA_CLIENT_TEST_SASL_MECHANISM").as_deref() {
        Err(env::VarError::NotPresent) | Ok("plain") => Sasl::plain(username, password),
        Ok("scram_sha_256") => Sasl::scram_sha_256(username, password),
        Ok("scram_sha_512") => Sasl::scram_sha_512(username, password),
        Ok(_) | Err(env::VarError::NotUnicode(_)) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "KAFKA_CLIENT_TEST_SASL_MECHANISM must be plain, scram_sha_256, or scram_sha_512",
            )
            .into());
        }
    };
    match env::var("KAFKA_CLIENT_TEST_SECURITY").as_deref() {
        Err(env::VarError::NotPresent) | Ok("sasl_plaintext") => Ok(Security::sasl_plaintext(sasl)),
        Ok("sasl_tls") => Ok(Security::sasl_tls(tls_from_environment()?, sasl)),
        Ok(_) | Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKA_CLIENT_TEST_SECURITY must be sasl_plaintext or sasl_tls",
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

fn required_environment(name: &str) -> Result<String, io::Error> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("set required smoke environment variable {name}"),
        )
    })
}
