//! Declarative expectations facade for hosted classic rejoin execution.

#[path = "expectations/capabilities.rs"]
mod capabilities;
#[path = "expectations/ownership.rs"]
mod ownership;

pub(super) use capabilities::{CALLS, CAPABILITIES, FIXTURE_FORBIDDEN, METHODS};
pub(super) use ownership::{AUTHORITIES, GROUP_ROOT, LINEAR, MIRRORS, MUTATIONS};
