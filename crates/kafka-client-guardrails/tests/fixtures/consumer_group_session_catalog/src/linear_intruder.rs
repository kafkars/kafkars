//! Cloneable group-session owners forbidden by this fixture.

#[derive(Clone, Copy)]
struct GroupSessionCatalog;

#[derive(Clone, Copy)]
struct ClassicGroupOwner;

#[derive(Clone, Copy)]
struct ClassicGroupCycleCandidate;

#[derive(Clone, Copy)]
struct CandidateMember;

#[derive(Clone, Copy)]
struct PreparedClassicGroupInstall;

#[derive(Clone, Copy)]
struct PreparedClassicGroupRevoke;
