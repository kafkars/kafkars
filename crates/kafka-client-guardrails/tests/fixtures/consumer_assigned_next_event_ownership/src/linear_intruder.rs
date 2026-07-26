//! Deliberately cloneable assigned next-event lifecycle owners.

#[derive(Clone, Copy)]
struct AssignedConsumerNextEvent;

#[derive(Clone, Copy)]
struct AssignedConsumerEventSignal;

#[derive(Clone, Copy)]
struct AssignedConsumerEventTicket;
