//! Deliberately cloneable assigned-receive lifecycle owners.

#[derive(Clone, Copy)]
struct AssignedConsumerRecv;

#[derive(Clone, Copy)]
struct AssignedConsumerRecvSignal;

#[derive(Clone, Copy)]
struct AssignedConsumerRecvTicket;

#[derive(Clone, Copy)]
struct AssignedConsumerCompletionPorts;

#[derive(Clone, Copy)]
struct AssignedConsumerPublishTicket;

#[derive(Clone, Copy)]
struct RecvAssignedBatch;
