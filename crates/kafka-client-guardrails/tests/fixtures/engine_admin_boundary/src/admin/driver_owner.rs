//! Reviewed concrete handoff fixture.

use crate::driver;

fn retains_one_call() {
    let _ = driver::OWNER;
}
