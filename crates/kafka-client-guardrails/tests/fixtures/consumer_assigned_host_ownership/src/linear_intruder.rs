//! Deliberately cloneable assigned-host lifecycle owners.

#[derive(Clone, Copy)]
struct AssignedConsumerShardState;

#[derive(Clone, Copy)]
struct AssignedConsumerShardOwner;

#[derive(Clone, Copy)]
struct AssignedConsumerPort;

#[derive(Clone, Copy)]
struct AssignedConsumerAccepted;

#[derive(Clone, Copy)]
struct AssignedConsumerReclaimRejection;

#[derive(Clone, Copy)]
struct StartedEngineHost;
