//! Declarative facade for client-quota alteration values and observation.

mod alteration;
mod builder;
mod entity;
mod operation;
mod result;

pub use alteration::{ClientQuotaAlteration, ClientQuotaAlterationOperation};
pub use builder::AlterClientQuotasBuilder;
pub use entity::ClientQuotaEntity;
pub use operation::AlterClientQuotas;
pub use result::AlterClientQuotasResult;

#[cfg(test)]
mod alteration_test;
#[cfg(test)]
mod entity_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
