//! Forbidden cloneable transaction facade lifecycle owners.

#[derive(Clone, Copy)]
struct TransactionalProducerInitializer;

#[derive(Clone, Copy)]
struct TransactionInitialization;

#[derive(Clone, Copy)]
struct TransactionalProducerEngine;

#[derive(Clone, Copy)]
struct TransactionalProducerBuilder;

#[derive(Clone, Copy)]
struct InitializeTransactionalProducer;

#[derive(Clone, Copy)]
struct TransactionalProducer;
