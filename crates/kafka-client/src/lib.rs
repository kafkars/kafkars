//! Provisional idiomatic Rust facade over the shared Kafka client engine.
//!
//! This crate is an executable API sketch. It performs no Kafka network I/O and
//! is not publishable under its current package or library name.

#![forbid(unsafe_code)]

mod admin;
mod client;
mod consumer;
mod error;
mod operation;
mod producer;
mod record;
mod transaction;

pub use admin::{Admin, BatchResult, CreateTopics, NewTopic};
pub use client::{Client, ClientBuilder, Shutdown};
pub use consumer::{
    AssignedConsumer, AssignedConsumerBuilder, Checkpoint, Commit, Consumer, ConsumerBuilder,
    ConsumerControl, ConsumerRecord, NextBatch, OffsetReset, RecordBatch,
};
pub use error::{ErrorKind, KafkaError};
pub use kafka_client_core::DeliveryStatus;
pub use operation::Operation;
pub use producer::{
    BatchDelivery, Delivery, Flush, Producer, ProducerBuilder, RecordMetadata, Send, SendBatch,
    TrySendError,
};
pub use record::{Header, Record};
pub use transaction::{
    AbortTransaction, BeginTransaction, BeginTransactionProducer, CommitTransaction, Transaction,
    TransactionalProducer, TransactionalProducerBuilder,
};
