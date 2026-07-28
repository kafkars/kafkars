//! Declarative facade for generated Produce request and `RecordBatch` ownership.

mod materialization;
#[cfg(test)]
mod materialization_test;
mod request;
#[cfg(test)]
mod request_test;

pub(crate) use materialization::{
    materialize_explicit_produce_batch, materialize_explicit_produce_batch_with_compression,
    materialize_transactional_produce_batch,
};
pub(crate) use request::MaterializedProduce;
