//! Provisional idiomatic Rust facade over the shared Kafka client engine.
//!
//! This crate is an executable API sketch. It performs no Kafka network I/O and
//! remains unpublished until the native producer reaches its qualification gate.

#![forbid(unsafe_code)]

mod admin;
mod bridge;
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
pub use error::{DeliveryStatus, ErrorKind, KafkaError};
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

#[cfg(test)]
mod client_test;
#[cfg(test)]
mod error_test;
