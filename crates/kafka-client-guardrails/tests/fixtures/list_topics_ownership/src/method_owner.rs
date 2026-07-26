//! Allowed public `ListTopics` internal-topic option call-site fixture.

struct Request;

impl Request {
    fn with_include_internal(self, _include: bool) -> Self {
        self
    }
}

fn select_internal_topics(request: Request) -> Request {
    request.with_include_internal(true)
}
