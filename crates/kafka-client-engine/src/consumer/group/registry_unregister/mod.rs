//! Declarative dormant group-unregistration owner and sibling evidence surface.

mod owner;

#[cfg(test)]
mod owner_test;

pub(super) use owner::GroupConsumerDormantUnregisterError;
