//! Cloneable assignment owners forbidden by the fixture.

#[derive(Clone, Copy)]
struct AssignedTopics;

#[derive(Clone, Copy)]
struct PreparedAssignedTopicsReplacement;

#[derive(Clone, Copy)]
struct PreparedAssignedTopicsAddition;

#[derive(Clone, Copy)]
struct PreparedAssignedTopicsRemoval;
