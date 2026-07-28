//! Opt-in real-cluster rejection and redaction coverage for SASL/PLAIN.

#[path = "real_broker_support/error.rs"]
mod error;
#[path = "real_broker_support/operation.rs"]
mod operation;

use std::{env, io};

use error::TestError;
use kafka_client::{Client, ErrorKind, Sasl, Security};
use operation::wait_within;

#[test]
#[ignore = "requires SASL smoke environment variables and an authenticated Kafka cluster"]
fn public_sasl_plain_rejects_wrong_password_without_exposing_credentials() {
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
        .client_id("kafka-client-real-sasl-rejection-smoke")
        .security(Security::sasl_plaintext(Sasl::plain(
            username,
            attempted_password.clone(),
        )))
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

fn required_environment(name: &str) -> Result<String, io::Error> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("set required smoke environment variable {name}"),
        )
    })
}
