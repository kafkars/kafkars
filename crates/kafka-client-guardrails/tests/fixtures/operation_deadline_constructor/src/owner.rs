//! Deliberately allowed construction of the paired operation deadline.

struct OperationDeadline;

impl OperationDeadline {
    fn from_boundary_parts() -> Self {
        Self
    }
}

fn capture_boundary() {
    let _deadline = OperationDeadline::from_boundary_parts();
}
