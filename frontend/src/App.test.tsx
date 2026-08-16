import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import { StrictMode } from "react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { MatchDashboard } from "./components/MatchDashboard";
import { formatClock } from "./components/MatchList";
import type { CanonicalEvent, MatchState } from "./types";

const firstState: MatchState = {
  match_id: "match-1", period: 1, match_clock_seconds: 65, latest_sequence: 1,
  home_team: { id: "home", name: "Home FC" }, away_team: { id: "away", name: "Away FC" },
  score: { home: 0, away: 0 }, updated_at: "2026-01-01T00:00:00Z", total_processed_events: 1,
  possession_team: { id: "home", name: "Home FC" }, event_counts: { Pass: 1 }, last_event_id: "event-1"
};

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  close = vi.fn(() => this.onclose?.());

  constructor(public readonly url: string) { MockWebSocket.instances.push(this); }

  emit(message: unknown): void { this.onmessage?.({ data: JSON.stringify(message) } as MessageEvent<string>); }
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
}

function errorResponse(detail: string): Response {
  return new Response(JSON.stringify({ detail }), { status: 503, headers: { "Content-Type": "application/json" } });
}

function event(sequence: number): CanonicalEvent {
  return {
    event_id: `event-${sequence}`, match_id: "match-1", sequence, period: 1,
    match_clock_seconds: sequence, event_type: "Pass", team: firstState.home_team,
    player: null, location: null, possession_id: null, possession_team: firstState.home_team,
    metadata: {}, source: "test", source_event_id: `event-${sequence}`
  };
}

beforeEach(() => {
  MockWebSocket.instances = [];
  vi.stubGlobal("WebSocket", MockWebSocket);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("dashboard", () => {
  it("formats fractional provider clocks as readable elapsed whole seconds", () => {
    expect(formatClock(3003.68)).toBe("50:03");
  });

  it("lists matches and switches the durable match view", async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes("/matches?")) return Promise.resolve(jsonResponse({ matches: [
        { ...firstState, match_id: "match-1" },
        { ...firstState, match_id: "match-2", home_team: { id: "other", name: "Other FC" } }
      ], next_after_match_id: null }));
      if (url.endsWith("/match-1")) return Promise.resolve(jsonResponse(firstState));
      if (url.endsWith("/match-2")) return Promise.resolve(jsonResponse({ ...firstState, match_id: "match-2", home_team: { id: "other", name: "Other FC" } }));
      return Promise.resolve(jsonResponse({ events: [], next_after_sequence: null }));
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(<App />);

    expect(await screen.findByRole("button", { name: /home fc/i })).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /other fc/i })).toBeInTheDocument();
    expect(screen.getByText("ID match-1")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /other fc/i }));
    await waitFor(() => expect(within(screen.getByLabelText("Current score")).getByText("Other FC")).toBeInTheDocument());
    expect(MockWebSocket.instances[0].close).toHaveBeenCalled();
  });

  it("renders empty and unavailable match-list states", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(jsonResponse({ matches: [], next_after_match_id: null }))));
    const { unmount } = render(<App />);
    expect(await screen.findByText("No matches are available yet.")).toBeInTheDocument();
    unmount();

    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(errorResponse("durable state unavailable"))));
    render(<App />);
    expect(await screen.findByText("durable state unavailable")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Try again" })).toBeInTheDocument();
  });

  it("does not treat Strict Mode's cancelled first request as an API outage", async () => {
    vi.stubGlobal("fetch", vi.fn((url: string) => Promise.resolve(jsonResponse(
      url.includes("/matches?") ? { matches: [{ ...firstState }], next_after_match_id: null }
        : url.includes("/events?") ? { events: [], next_after_sequence: null } : firstState
    ))));
    render(<StrictMode><App /></StrictMode>);

    expect(await screen.findByRole("button", { name: /home fc/i })).toBeInTheDocument();
    expect(screen.queryByText("MatchStream API is unavailable. Check that the API service is running.")).toBeNull();
  });

  it("marks the view live only after a WebSocket snapshot and ignores stale updates", async () => {
    const fetchMock = vi.fn((url: string) => Promise.resolve(jsonResponse(
      url.includes("/events?") ? { events: [], next_after_sequence: null } : firstState
    )));
    vi.stubGlobal("fetch", fetchMock);
    render(<MatchDashboard matchId="match-1" />);

    const score = await screen.findByLabelText("Current score");
    expect(within(score).getByText("Home FC")).toBeInTheDocument();
    expect(screen.getAllByText("CONNECTING")).not.toHaveLength(0);
    const socket = MockWebSocket.instances[0];
    socket.emit({ type: "snapshot", protocol_version: 1, state: firstState });
    expect((await screen.findAllByText("LIVE"))).not.toHaveLength(0);
    socket.emit({ type: "state_update", protocol_version: 1, state: { ...firstState, latest_sequence: 2, total_processed_events: 2 } });
    const processedEvents = screen.getByText("Processed events").parentElement;
    expect(await within(processedEvents!).findByText("2")).toBeInTheDocument();
    socket.emit({ type: "state_update", protocol_version: 1, state: firstState });
    expect(within(processedEvents!).getByText("2")).toBeInTheDocument();
    socket.close();
    expect((await screen.findAllByText("RECONNECTING"))).not.toHaveLength(0);
    await waitFor(() => expect(MockWebSocket.instances).toHaveLength(2));
    MockWebSocket.instances[1].emit({
      type: "snapshot",
      protocol_version: 1,
      state: { ...firstState, latest_sequence: 3, total_processed_events: 3 }
    });
    expect(await within(processedEvents!).findByText("3")).toBeInTheDocument();
  });

  it("keeps timeline history ordered and appends only new events", async () => {
    let historyRequests = 0;
    vi.stubGlobal("fetch", vi.fn((url: string) => {
      if (!url.includes("/events?")) return Promise.resolve(jsonResponse(firstState));
      historyRequests += 1;
      return Promise.resolve(jsonResponse({
        events: historyRequests === 1 ? [event(2), event(1)] : [event(2), event(3)],
        next_after_sequence: null
      }));
    }));
    render(<MatchDashboard matchId="match-1" />);

    await waitFor(() => expect(screen.getAllByRole("listitem")).toHaveLength(2));
    expect(screen.getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "#1 · 0:01PassHome FC", "#2 · 0:02PassHome FC"
    ]);
    MockWebSocket.instances[0].emit({
      type: "state_update",
      protocol_version: 1,
      state: { ...firstState, latest_sequence: 3, total_processed_events: 3 }
    });
    await waitFor(() => expect(screen.getAllByRole("listitem")).toHaveLength(3));
    expect(screen.getAllByRole("listitem")[2]).toHaveTextContent("#3 · 0:03");
  });
});
