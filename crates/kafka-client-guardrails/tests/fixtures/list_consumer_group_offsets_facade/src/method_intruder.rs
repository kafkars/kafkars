//! Forbidden second stable-offset option call-site fixture.

struct Request;

impl Request {
    fn with_require_stable(self, _require_stable: bool) -> Self {
        self
    }
}

fn invent_second_policy_surface(request: Request) -> Request {
    request.with_require_stable(false)
}
