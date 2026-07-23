//! Raw identifier spelling cannot disguise a shadowed trusted macro root.

mod evil {
    pub use ::std::include as format;
}

use crate::evil as r#std;

r#std::format!("hidden.inc");
