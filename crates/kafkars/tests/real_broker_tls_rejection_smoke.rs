//! Opt-in real-cluster certificate-identity rejection through the public facade.

#[path = "real_broker_support/error.rs"]
mod error;
#[path = "real_broker_support/operation.rs"]
mod operation;

use std::{env, fs, io, time::Duration};

use error::TestError;
use kafkars::{Client, DeliveryStatus, ErrorKind, Security, Tls};
use operation::{wait_within, wait_within_for};

const REJECTION_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(45);

#[test]
#[ignore = "requires TLS smoke environment variables and a TLS-enabled Kafka cluster"]
fn public_tls_rejects_mismatched_server_identity() {
    run().unwrap_or_else(|error| panic!("real Kafka TLS rejection smoke failed: {error}"));
}

fn run() -> Result<(), TestError> {
    let bootstrap = required_environment("KAFKA_CLIENT_TEST_TLS_MISMATCH_BOOTSTRAP")?;
    let certificate = fs::read(required_environment("KAFKA_CLIENT_TEST_TLS_CA_PEM")?)?;
    let client = Client::builder()
        .bootstrap_servers(
            bootstrap
                .split(',')
                .map(str::trim)
                .filter(|server| !server.is_empty()),
        )
        .client_id("kafka-client-real-tls-rejection-smoke")
        .security(Security::tls(Tls::custom_roots_pem(certificate.clone())))
        .build()?;

    let readiness = wait_within_for(
        client.ready(),
        "mismatched TLS identity readiness",
        REJECTION_OBSERVATION_TIMEOUT,
    );
    let shutdown = wait_within(client.shutdown(), "mismatched TLS identity client shutdown");
    let readiness = readiness?;
    shutdown??;

    let ready_error = readiness
        .err()
        .ok_or_else(|| io::Error::other("mismatched TLS identity unexpectedly authenticated"))?;
    if ready_error.kind() != ErrorKind::Transport
        || ready_error.delivery_status() != Some(DeliveryStatus::NotSent)
    {
        return Err(io::Error::other(format!(
            "mismatched TLS identity returned unexpected public error facts: {ready_error:?}"
        ))
        .into());
    }
    let public_error = format!("{ready_error:?} {ready_error}");
    if public_error.contains("BEGIN CERTIFICATE")
        || public_error.contains(String::from_utf8_lossy(&certificate).as_ref())
    {
        return Err(io::Error::other("TLS verification error exposed certificate material").into());
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
