//! Declarative private bridge for configuration-resource listings.

mod engine;
mod operation;
mod result;

pub(crate) use operation::AdminListConfigResources;

#[cfg(test)]
mod result_test;
