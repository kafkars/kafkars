//! Construction of the cloneable public admin handle from its private engine bridge.

use super::Admin;
use crate::bridge::admin::AdminEngine;

impl Admin {
    pub(crate) const fn new(engine: AdminEngine) -> Self {
        Self { engine }
    }
}
