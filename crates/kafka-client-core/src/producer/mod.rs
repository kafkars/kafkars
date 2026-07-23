//! Atomic producer admission, retained capacity, and terminal settlement.

mod lifecycle;
mod machine;

pub use machine::ProducerMachine;
