//! Invalid unit-test declaration fixture.

mod ungated_test;

#[cfg(test)]
#[path = "decoy.rs"]
mod redirected_test;

#[cfg(test)]
#[cfg(any())]
mod disabled_test;

#[cfg(test)]
#[cfg_attr(test, path = "decoy.rs")]
mod conditional_test;

#[cfg(test)]
#[cfg_attr(test, cfg(any()))]
mod cfg_attr_disabled_test;
