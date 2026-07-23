//! Invalid nested cfg predicates for inline unit tests.

#[cfg(any(doc, test))]
mod scenarios {
    fn scenario() {}
}

#[cfg_attr(all(), test)]
fn conditional_test_attribute() {}
