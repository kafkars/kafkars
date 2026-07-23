//! Reachable source attempting to import generated Rust outside inspection.

include!(concat!(env!("OUT_DIR"), "/generated.rs"));
