import { describe, expect, it } from "vitest";

import { mergeEvents, shouldReconcile } from "./useMatchSession";
import type { CanonicalEvent } from "../types";

const event = (sequence: number, eventId = `event-${sequence}`): CanonicalEvent => ({
  event_id: eventId,
  match_id: "match-1",
  sequence,
  period: 1,
  match_clock_seconds: sequence,
  event_type: "Pass",
  team: null,
  player: null,
  location: null,
  possession_id: null,
  possession_team: null,
  metadata: {},
  source: "test",
  source_event_id: eventId
});

describe("match session ordering", () => {
  it("deduplicates events and keeps a deterministic sequence order", () => {
    expect(mergeEvents([event(2), event(1)], [event(2), event(3)])).toEqual([event(1), event(2), event(3)]);
  });

  it("reconciles only a forward sequence gap", () => {
    expect(shouldReconcile(4, 6)).toBe(true);
    expect(shouldReconcile(4, 5)).toBe(false);
    expect(shouldReconcile(4, 4)).toBe(false);
    expect(shouldReconcile(null, 4)).toBe(false);
  });
});
