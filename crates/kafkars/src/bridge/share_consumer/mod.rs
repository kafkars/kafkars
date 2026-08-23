//! Declarative bridge for unique share-member ownership and observation.

mod close;
mod registration;
mod state;

pub(crate) use close::ShareConsumerClose;
pub(crate) use registration::ShareConsumerEngine;
pub(crate) use registration::translate_registration_kind;

#[cfg(test)]
mod close_test;
#[cfg(test)]
mod registration_test;
#[cfg(test)]
mod state_test;
