//! Glob imports cannot hide the unbounded channel constructor.

use std::sync::mpsc::*;

fn construct_unbounded_channel() {
    let (_sender, _receiver) = channel::<u8>();
}
