//! Sequential assignment-fenced resolution of missing group positions.

mod error;
mod machine;
mod model;
mod transition;

pub use error::{GroupPositionResetApplyError, GroupPositionResetMachineError};
pub use machine::GroupPositionResetMachine;
pub use model::{
    GroupPositionResetEffect, GroupPositionResetFailure, GroupPositionResetInput,
    GroupPositionResetState, GroupPositionResetTerminal, GroupPositionResetTransition,
};
