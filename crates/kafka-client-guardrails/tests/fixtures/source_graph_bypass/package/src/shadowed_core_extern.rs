//! Extern-crate aliases cannot impersonate a trusted macro root.

pub use ::std::include as format;

extern crate self as core;

core::format!("hidden.inc");
