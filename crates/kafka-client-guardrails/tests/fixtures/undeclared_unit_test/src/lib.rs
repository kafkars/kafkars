//! Invalid unit-test declaration fixture.

mod ungated_test;

#[cfg(test)]
#[path = "decoy.rs"]
mod redirected_test;
