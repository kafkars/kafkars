//! Deliberately cloneable assigned-consumer claim capabilities.

#[derive(Clone, Copy)]
struct AssignedConsumerClaimSlot;

#[derive(Clone, Copy)]
struct AssignedConsumerAdmissionCloser;

#[derive(Clone, Copy)]
struct AssignedConsumerHandle;

#[derive(Clone, Copy)]
struct AssignedConsumerTryCloseAccepted;
