//! An inner alias cannot leak a forbidden meaning into its outer namesake.

use core as platform;

fn inner_scope() {
    use std as platform;

    let _value: Option<platform::fmt::Error> = None;
}

fn allowed_outer_scope() -> Option<platform::net::IpAddr> {
    None
}
