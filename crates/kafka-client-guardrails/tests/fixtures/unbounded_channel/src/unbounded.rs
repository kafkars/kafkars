//! Deliberately invalid unbounded-channel capability fixture.

use std::sync::mpsc;

fn construct_unbounded_channel() {
    let (_sender, _receiver) = mpsc::channel::<u8>();
}
