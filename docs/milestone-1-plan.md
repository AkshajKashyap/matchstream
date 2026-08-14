# Milestone 1 implementation plan

1. Define a small, source-independent canonical event model using typed,
   validated standard-library dataclasses.
2. Implement a StatsBomb Open Data parser that validates documented required
   fields, safely extracts selected optional fields, and returns events in a
   deterministic order.
3. Implement a replay component with synchronous and asynchronous iteration,
   configurable speed, and a no-wait path for tests and broker adapters.
4. Add compact StatsBomb-style fixtures and focused unit tests for conversion,
   validation, ordering, and replay timing.
5. Document the boundary between source parsing and replay, run lint/tests,
   and demonstrate the no-wait replay locally.
