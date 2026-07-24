//! Deliberately cloneable assigned-consumer event owners.

#[derive(Clone, Copy)]
struct AssignedConsumerEventStore;

#[derive(Clone, Copy)]
struct AssignedConsumerEvent;

#[derive(Clone, Copy)]
struct PreparedEventClaims;
