//! Forbidden cloneable group-offset alteration ownership fixtures.

#[derive(Clone, Copy)]
struct GroupOffsetAlterCall;

#[derive(Clone, Copy)]
struct GroupOffsetAlterCallAdmissionFailure;

#[derive(Clone, Copy)]
struct GroupOffsetAlterTerminal;

#[derive(Clone, Copy)]
struct RecoveredGroupOffsetAlterCall;
