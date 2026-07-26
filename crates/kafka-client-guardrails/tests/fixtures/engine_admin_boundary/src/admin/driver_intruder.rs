//! Deliberately invalid sibling driver access fixture.

use crate::driver;

fn reaches_transport_without_ownership() {
    let _ = driver::OWNER;
}
