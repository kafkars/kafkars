//! Declarative facade for `ShareFetch` broker, generation, and session identities.

mod broker;
mod generation;
mod session;

pub use broker::ShareFetchBrokerId;
pub use generation::{
    ShareAcquisitionGeneration, ShareConnectionGeneration, ShareFetchAssignmentGeneration,
    ShareRouteGeneration,
};
pub use session::{ShareFetchAttempt, ShareFetchSessionEpoch, ShareFetchSessionFence};
