//! Share-consumer host-turn execution and wake integration.

mod drive;
#[cfg(test)]
mod drive_test;
mod wake;

pub(super) use drive::{ShareConsumerProgress, drive};
