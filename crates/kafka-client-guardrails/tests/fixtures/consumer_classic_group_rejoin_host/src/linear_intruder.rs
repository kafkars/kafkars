//! Deliberately cloneable classic rejoin execution owners.

#[derive(Clone, Copy)]
struct ClassicGroupRejoinState;
#[derive(Clone, Copy)]
struct ClassicGroupRejoinExecution;
#[derive(Clone, Copy)]
struct PreparedClassicRejoinInstall;
#[derive(Clone, Copy)]
struct PendingClassicRejoinJoin;
#[derive(Clone, Copy)]
struct ClassicRejoinPostCore;
#[derive(Clone, Copy)]
struct ClassicRejectionPostCore;
#[derive(Clone, Copy)]
struct ClassicSyncRejectionFailure;
