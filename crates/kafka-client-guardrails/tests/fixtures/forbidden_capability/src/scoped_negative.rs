//! An inner alias cannot erase the forbidden meaning of its outer namesake.

use std as platform;

fn inner_scope() {
    use core as platform;

    let _value: Option<platform::fmt::Error> = None;
}

fn forbidden_outer_scope() -> Option<platform::net::TcpStream> {
    None
}
