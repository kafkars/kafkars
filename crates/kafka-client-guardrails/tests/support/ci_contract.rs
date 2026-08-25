//! Structural CI workflow, qualification, and architecture-entrypoint contracts.

#![allow(dead_code, unused_imports)]

#[path = "ci_contract/architecture.rs"]
mod architecture;
#[path = "ci_contract/qualification.rs"]
mod qualification;
#[path = "ci_contract/shared.rs"]
mod shared;
#[path = "ci_contract/workflow.rs"]
mod workflow;
#[path = "ci_contract/workflow_steps.rs"]
mod workflow_steps;

pub(crate) use architecture::violations as architecture_script_violations;
pub(crate) use qualification::violations as qualification_workflow_violations;
pub(crate) use workflow::violations as workflow_violations;
