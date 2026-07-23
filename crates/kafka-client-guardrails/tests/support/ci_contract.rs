//! Structural GitHub workflow and sibling-checkout action contracts.

#[path = "ci_contract/action.rs"]
mod action;
#[path = "ci_contract/architecture.rs"]
mod architecture;
#[path = "ci_contract/revisions.rs"]
mod revisions;
#[path = "ci_contract/shared.rs"]
mod shared;
#[path = "ci_contract/workflow.rs"]
mod workflow;
#[path = "ci_contract/workflow_steps.rs"]
mod workflow_steps;

pub(crate) use action::violations as action_violations;
pub(crate) use architecture::violations as architecture_script_violations;
pub(crate) use revisions::violations as revision_file_violations;
pub(crate) use workflow::violations as workflow_violations;
