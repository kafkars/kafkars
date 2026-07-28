//! Shared boxed error vocabulary for opt-in real-cluster workflows.

pub(crate) type TestError = Box<dyn std::error::Error>;
