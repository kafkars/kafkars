//! Domain-neutral admin policy fixture.

use crate::completion;

fn owns_only_admin_policy() {
    let _ = completion::OWNER;
}
