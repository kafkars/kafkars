//! Producer admission and terminal-result translation at the private engine seam.

pub(crate) mod admission;
pub(crate) mod delivery;
pub(crate) mod flush;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod flush_test;
