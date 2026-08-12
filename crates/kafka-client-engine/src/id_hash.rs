//! Deterministic hashing for trusted monotonic internal identities.

use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
};

pub(crate) type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdHasher>>;

pub(crate) const fn id_map<K, V>() -> IdMap<K, V> {
    HashMap::with_hasher(BuildHasherDefault::new())
}

#[derive(Default)]
pub(crate) struct IdHasher {
    state: u64,
}

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut state = self.state ^ 0xcbf2_9ce4_8422_2325;
        for byte in bytes {
            state ^= u64::from(*byte);
            state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.state = mix(state);
    }

    fn write_u64(&mut self, value: u64) {
        self.state = mix(self.state ^ value.wrapping_add(0x9e37_79b9_7f4a_7c15));
    }

    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
