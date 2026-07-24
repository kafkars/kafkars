//! Deliberately cloneable direct-Fetch executor owners.

#[derive(Clone, Copy)]
struct FetchAttemptDeadline;

#[derive(Clone, Copy)]
struct PreparedFetchExecution;

#[derive(Clone, Copy)]
struct FetchSubmission;

#[derive(Clone, Copy)]
struct ActiveFetchReservation;

#[derive(Clone, Copy)]
struct ExecutorSeal;

#[derive(Clone, Copy)]
struct DirectFetchExecutor;

#[derive(Clone, Copy)]
struct RetainedFetchFault;

#[derive(Clone, Copy)]
struct FetchReclaimFailure;

#[derive(Clone, Copy)]
struct FetchShutdownRecovery;

#[derive(Clone, Copy)]
struct FetchTerminalFact;
