//! Forbidden renamed duplication trait.

use std::clone::Clone as Duplicate;

struct AliasTarget;

impl Duplicate for AliasTarget {
    fn clone(&self) -> Self {
        Self
    }
}
