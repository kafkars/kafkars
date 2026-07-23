# Style

- Prefer a named state transition to a hidden callback or side effect.
- State variants contain only data valid in that state.
- Time enters deterministic code as an explicit absolute value.
- Every retained object has an explicit byte/count owner.
- Public operations document admission, deadline, cancellation, drop, terminal
  completion, and shutdown semantics.
- The curated facade uses Kafka vocabulary consistently: record, header, topic,
  partition, offset, checkpoint, assignment, producer, consumer, admin.
- Keep modules small enough that one ownership concept can be reviewed at once.
- `unsafe` is forbidden throughout the repository.
- Examples are API tests, not marketing pseudocode.
