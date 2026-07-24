//! Cluster-scoped client construction and child-handle ownership.

use crate::admin::Admin;
use crate::bridge::ClientEngine;
use crate::consumer::{AssignedConsumerBuilder, ConsumerBuilder};
use crate::error::{ErrorKind, KafkaError};
use crate::operation::Operation;
use crate::producer::Compression;
use crate::producer::ProducerBuilder;
use crate::transaction::TransactionalProducerBuilder;

/// Builder for one shared cluster, security, and execution context.
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    producer_compression: Compression,
}

impl ClientBuilder {
    /// Replaces the logical bootstrap endpoint set.
    pub fn bootstrap_servers<I, S>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.bootstrap_servers = servers.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the client identifier reported to Kafka.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Selects `RecordBatch` compression for this client's producer owner.
    #[must_use]
    pub const fn producer_compression(mut self, compression: Compression) -> Self {
        self.producer_compression = compression;
        self
    }

    /// Validates local configuration and starts the default host.
    pub fn build(self) -> Result<Client, KafkaError> {
        if self.bootstrap_servers.is_empty() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "at least one bootstrap server is required",
            ));
        }

        let engine = ClientEngine::start(self.bootstrap_servers, self.producer_compression)?;
        Ok(Client {
            engine,
            client_id: self.client_id,
        })
    }
}

/// Cheaply cloneable cluster-scoped client handle.
#[derive(Debug, Clone)]
pub struct Client {
    engine: ClientEngine,
    client_id: Option<String>,
}

impl Client {
    /// Begins client construction.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Returns the configured client identifier.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns validated bootstrap endpoints.
    pub fn bootstrap_servers(&self) -> &[String] {
        self.engine.bootstrap_servers()
    }

    /// Begins construction of a thread-safe producer.
    pub fn producer(&self) -> ProducerBuilder {
        ProducerBuilder::new(self.engine.producer())
    }

    /// Begins construction of a uniquely controlled group consumer.
    pub fn consumer(&self, group_id: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder::new(self.clone(), group_id.into())
    }

    /// Begins construction of a directly assigned consumer.
    pub fn assigned_consumer(&self) -> AssignedConsumerBuilder {
        AssignedConsumerBuilder::new(self.engine.clone())
    }

    /// Returns a cheap thread-safe admin handle.
    pub fn admin(&self) -> Admin {
        Admin::new(self.engine.admin())
    }

    /// Begins construction of a uniquely controlled transactional producer.
    pub fn transactional_producer(
        &self,
        transactional_id: impl Into<String>,
    ) -> TransactionalProducerBuilder {
        TransactionalProducerBuilder::new(self.clone(), transactional_id.into())
    }

    /// Initiates graceful client shutdown.
    pub fn shutdown(&self) -> Shutdown {
        let _ = &self.engine;
        Operation::ready(Ok(()))
    }
}

/// Graceful shutdown operation.
pub type Shutdown = Operation<Result<(), KafkaError>>;
