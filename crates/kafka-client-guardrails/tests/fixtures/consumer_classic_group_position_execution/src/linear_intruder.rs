//! Deliberately cloneable classic-group position owners.

#[derive(Clone, Copy)]
struct ClassicGroupPositionPrepared;
#[derive(Clone, Copy)]
struct ClassicGroupPositionHandoff;
#[derive(Clone, Copy)]
struct ClassicGroupPositionDriverOwned;
#[derive(Clone, Copy)]
struct ClassicGroupPositionCompleted;
#[derive(Clone, Copy)]
struct ClassicGroupPositionConfirmationPending;
#[derive(Clone, Copy)]
struct ClassicGroupPositionExecutionState;
#[derive(Clone, Copy)]
struct ClassicGroupPositionExecution;
