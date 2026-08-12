//! Deterministic internal identity-hash distribution scenarios.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use super::id_hash::IdHasher;

#[test]
fn monotonic_id_hashes_are_deterministic_and_distinct_across_capacity() {
    let hashes = (1_u64..=8_192).map(hash).collect::<BTreeSet<_>>();

    assert_eq!(hashes.len(), 8_192);
    assert_eq!(hash(1), hash(1));
    assert_ne!(hash(1), hash(2));
}

#[test]
fn composite_integer_hashes_retain_every_field() {
    let hashes = (0_usize..8_192)
        .map(|slot| hash((slot, 0_u64)))
        .collect::<BTreeSet<_>>();

    assert_eq!(hashes.len(), 8_192);
    assert_ne!(hash((7_usize, 0_u64)), hash((7_usize, 1_u64)));
}

fn hash(value: impl Hash) -> u64 {
    let mut hasher = IdHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}
