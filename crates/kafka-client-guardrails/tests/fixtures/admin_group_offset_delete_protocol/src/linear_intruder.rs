//! Forbidden cloneable group-offset deletion ownership fixtures.

#[derive(Clone, Copy)]
struct GroupOffsetDeleteCall;

#[derive(Clone, Copy)]
struct GroupOffsetDeleteCallAdmissionFailure;

#[derive(Clone, Copy)]
struct GroupOffsetDeleteTerminal;

#[derive(Clone, Copy)]
struct RecoveredGroupOffsetDeleteCall;
