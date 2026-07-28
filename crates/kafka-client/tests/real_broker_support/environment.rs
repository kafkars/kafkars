//! Shared plaintext real-cluster bootstrap environment.

use std::{env, io};

use kafka_client::{Client, ClientBuilder};

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

    require_plaintext_environment()?;

    Ok(Client::builder()
        .bootstrap_servers(servers)
        .client_id(client_id))
}

fn require_plaintext_environment() -> Result<(), TestError> {
    match env::var("KAFKA_CLIENT_TEST_SECURITY").as_deref() {
        Err(env::VarError::NotPresent) | Ok("plaintext") => Ok(()),
        Ok(_) | Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KAFKA_CLIENT_TEST_SECURITY must be plaintext",
        )
        .into()),
    }
}
