//! Forbidden cloneable assigned-event observers.

#[derive(Clone, Copy)]
struct AssignedConsumerNextEvent;

#[derive(Clone, Copy)]
struct NextAssignedEvent;
