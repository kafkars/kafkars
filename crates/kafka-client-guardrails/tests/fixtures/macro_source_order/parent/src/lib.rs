//! Parent importing an opaque macro before declaring external child modules.

#[macro_use]
extern crate evil;

mod conditional_decoy;
mod later_decoy;
mod local_before;

macro_rules! opaque {
    () => {};
}
