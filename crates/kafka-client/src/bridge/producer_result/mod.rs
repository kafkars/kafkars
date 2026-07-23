//! Producer admission and terminal-result translation at the private engine seam.

pub(crate) mod admission;
pub(crate) mod delivery;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod delivery_test;
