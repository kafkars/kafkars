//! Invalid crate-private glob re-export from a non-export owner.

mod owner;

pub(crate) use owner::*;
