//! Domain-neutral shared-driver mechanism accepted by the boundary.

use crate::driver_common::ReactorWake;

fn retain(wake: ReactorWake) {
    let _wake = wake;
}
