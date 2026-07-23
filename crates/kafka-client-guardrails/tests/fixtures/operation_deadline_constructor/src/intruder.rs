//! Deliberately forbidden construction outside monotonic capture.

struct OperationDeadline;

impl OperationDeadline {
    fn from_boundary_parts() -> Self {
        Self
    }
}

fn forge_pair() {
    let _deadline = OperationDeadline::from_boundary_parts();
}
