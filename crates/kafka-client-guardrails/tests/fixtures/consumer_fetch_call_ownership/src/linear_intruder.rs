//! Deliberately cloneable tracked Fetch lifecycle-owner fixture.

#[derive(Clone, Copy)]
struct PartitionFetchRequest;

#[derive(Clone, Copy)]
struct FetchAdmissionFailure;

#[derive(Clone, Copy)]
struct FetchCallAdmission;

#[derive(Clone, Copy)]
struct FetchCallPermit;

#[derive(Clone, Copy)]
struct TrackedFetchCall;

#[derive(Clone, Copy)]
struct SettledFetchCall;

#[derive(Clone, Copy)]
struct TrackedFetchCalls;

#[derive(Clone, Copy)]
struct FetchTerminal;

#[derive(Clone, Copy)]
struct FetchCompletionFailure;

#[derive(Clone, Copy)]
struct ForgottenFetchRequest;

#[derive(Clone, Copy)]
struct TrackedForgottenFetchCall;

#[derive(Clone, Copy)]
struct ForgottenFetchTerminal;

#[derive(Clone, Copy)]
struct ForgottenFetchConfirmation;

#[derive(Clone, Copy)]
struct ForgottenFetchSubmitFailure;

#[derive(Clone, Copy)]
struct ForgottenFetchCompletionFailure;

#[derive(Clone, Copy)]
struct PendingFetchConfirmation;

#[derive(Clone, Copy)]
struct FetchRestoreFailure;

#[derive(Clone, Copy)]
struct StaleFetchDrains;

#[derive(Clone, Copy)]
struct FetchRecovery;
