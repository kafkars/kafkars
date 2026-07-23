//! A facade lock cannot hide behind a local type name.

use std::sync::Mutex as FacadeLock;

fn construct_lock() -> FacadeLock<u8> {
    FacadeLock::new(1)
}
