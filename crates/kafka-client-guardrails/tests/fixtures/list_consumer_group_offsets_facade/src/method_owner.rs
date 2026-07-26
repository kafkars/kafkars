//! Allowed public stable-offset option call-site fixture.

struct Request;

impl Request {
    fn with_require_stable(self, _require_stable: bool) -> Self {
        self
    }
}

fn require_stable_offsets(request: Request) -> Request {
    request.with_require_stable(true)
}
