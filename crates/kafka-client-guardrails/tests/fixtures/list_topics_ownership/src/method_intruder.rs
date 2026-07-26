//! Forbidden second internal-topic option call-site fixture.

struct Request;

impl Request {
    fn with_include_internal(self, _include: bool) -> Self {
        self
    }
}

fn invent_second_policy_surface(request: Request) -> Request {
    request.with_include_internal(false)
}
