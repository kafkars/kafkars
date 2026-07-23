//! Two-stage imports cannot launder a macro through a shadowed builtin root.

mod evil {
    pub use ::std::include as format;
}

use crate::evil as std;
use std::format;

format!("hidden.inc");
