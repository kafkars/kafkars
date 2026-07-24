//! Deliberately constructs a publication port elsewhere.

fn violate(registry: Registry) {
    let _publisher = registry.publish_port(Ticket::Close);
}
