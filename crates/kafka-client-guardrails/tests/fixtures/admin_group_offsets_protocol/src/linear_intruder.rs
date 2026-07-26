//! Forbidden cloneable group-offset ownership fixtures.

#[derive(Clone, Copy)]
struct GroupOffsetsRequest;

#[derive(Clone, Copy)]
struct GroupOffsetsCall;

#[derive(Clone, Copy)]
struct GroupOffsetsCallAdmissionFailure;

#[derive(Clone, Copy)]
struct GroupOffsetsTerminal;

#[derive(Clone, Copy)]
struct RecoveredGroupOffsetsCall;
