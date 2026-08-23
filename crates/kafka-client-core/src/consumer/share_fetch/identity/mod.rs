//! Declarative facade for `ShareFetch` broker, generation, and session identities.

mod broker;
mod generation;
mod session;

pub use broker::ShareFetchBrokerId;
pub use generation::{ShareAcquisitionGeneration, ShareFetchAssignmentGeneration};
pub use session::{ShareFetchAttempt, ShareFetchSessionEpoch, ShareFetchSessionFence};
