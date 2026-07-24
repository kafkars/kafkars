//! Deliberately cloneable close-observation lifecycle owners.

#[derive(Clone, Copy)]
struct AssignedConsumerCloseObserver;

#[derive(Clone, Copy)]
struct AssignedConsumerCompletionNotifier;
