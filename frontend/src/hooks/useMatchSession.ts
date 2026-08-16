import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api";
import type { CanonicalEvent, ConnectionStatus, MatchState, StreamMessage } from "../types";

const MAX_TIMELINE_EVENTS = 50;
const MAX_RECONNECT_ATTEMPTS = 6;

export interface MatchSession {
  state: MatchState | null;
  events: CanonicalEvent[];
  loading: boolean;
  error: string | null;
  timelineError: string | null;
  connectionStatus: ConnectionStatus;
  retry(): void;
}

function newestSequence(events: CanonicalEvent[]): number {
  return events.at(-1)?.sequence ?? 0;
}

export function mergeEvents(previous: CanonicalEvent[], incoming: CanonicalEvent[]): CanonicalEvent[] {
  const deduplicated = new Map(previous.map((event) => [event.event_id, event]));
  incoming.forEach((event) => deduplicated.set(event.event_id, event));
  return [...deduplicated.values()].sort((left, right) => left.sequence - right.sequence).slice(-MAX_TIMELINE_EVENTS);
}

export function shouldReconcile(currentSequence: number | null, incomingSequence: number | null): boolean {
  return currentSequence !== null && incomingSequence !== null && incomingSequence > currentSequence + 1;
}

export function useMatchSession(matchId: string | null): MatchSession {
  const [state, setState] = useState<MatchState | null>(null);
  const [events, setEvents] = useState<CanonicalEvent[]>([]);
  const [loading, setLoading] = useState(Boolean(matchId));
  const [error, setError] = useState<string | null>(null);
  const [timelineError, setTimelineError] = useState<string | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>("DISCONNECTED");
  const [retryToken, setRetryToken] = useState(0);
  const stateRef = useRef<MatchState | null>(null);
  const eventsRef = useRef<CanonicalEvent[]>([]);

  const applyEvents = useCallback((incoming: CanonicalEvent[]) => {
    setEvents((previous) => {
      const next = mergeEvents(previous, incoming);
      eventsRef.current = next;
      return next;
    });
  }, []);

  const retry = useCallback(() => setRetryToken((value) => value + 1), []);

  useEffect(() => {
    if (!matchId) {
      stateRef.current = null;
      eventsRef.current = [];
      setState(null);
      setEvents([]);
      setLoading(false);
      setError(null);
      setConnectionStatus("DISCONNECTED");
      return undefined;
    }

    let active = true;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | undefined;
    let reconnectAttempts = 0;
    const controller = new AbortController();

    const replaceState = (next: MatchState) => {
      stateRef.current = next;
      setState(next);
    };

    const loadEventsAfter = async (afterSequence: number) => {
      try {
        const history = await api.getEvents(matchId, afterSequence, controller.signal);
        if (active) {
          applyEvents(history.events);
          setTimelineError(null);
        }
      } catch (caught) {
        if (active && !(caught instanceof DOMException && caught.name === "AbortError")) {
          setTimelineError("Recent events could not be refreshed.");
        }
      }
    };

    const reconcile = async () => {
      try {
        const authoritative = await api.getMatch(matchId, controller.signal);
        if (!active) return;
        replaceState(authoritative);
        await loadEventsAfter(newestSequence(eventsRef.current));
      } catch (caught) {
        if (active && !(caught instanceof DOMException && caught.name === "AbortError")) {
          setError(caught instanceof Error ? caught.message : "Unable to reconcile match state.");
        }
      }
    };

    const handleMessage = (message: StreamMessage) => {
      const incoming = message.state;
      const current = stateRef.current;
      if (message.type === "snapshot") {
        replaceState(incoming);
        void loadEventsAfter(Math.max(0, (incoming.latest_sequence ?? 0) - MAX_TIMELINE_EVENTS));
        setConnectionStatus("LIVE");
        return;
      }
      if (current !== null && current.latest_sequence !== null && incoming.latest_sequence !== null) {
        if (incoming.latest_sequence <= current.latest_sequence) return;
        if (shouldReconcile(current.latest_sequence, incoming.latest_sequence)) {
          void reconcile();
          return;
        }
      }
      replaceState(incoming);
      void loadEventsAfter(newestSequence(eventsRef.current));
    };

    const connect = () => {
      if (!active) return;
      setConnectionStatus(reconnectAttempts === 0 ? "CONNECTING" : "RECONNECTING");
      const nextSocket = new WebSocket(api.streamUrl(matchId));
      socket = nextSocket;
      nextSocket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data) as StreamMessage;
          if (message.type === "snapshot" || message.type === "state_update") handleMessage(message);
        } catch {
          if (active) setTimelineError("Received an invalid live update.");
        }
      };
      nextSocket.onerror = () => {
        if (socket === nextSocket) nextSocket.close();
      };
      nextSocket.onclose = () => {
        if (!active || socket !== nextSocket) return;
        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
          setConnectionStatus("DISCONNECTED");
          return;
        }
        const delay = Math.min(500 * 2 ** reconnectAttempts, 8000);
        reconnectAttempts += 1;
        setConnectionStatus("RECONNECTING");
        reconnectTimer = window.setTimeout(connect, delay);
      };
    };

    const initialize = async () => {
      setLoading(true);
      setError(null);
      setTimelineError(null);
      setState(null);
      setEvents([]);
      stateRef.current = null;
      eventsRef.current = [];
      try {
        const snapshot = await api.getMatch(matchId, controller.signal);
        if (!active) return;
        replaceState(snapshot);
        await loadEventsAfter(Math.max(0, (snapshot.latest_sequence ?? 0) - MAX_TIMELINE_EVENTS));
        connect();
      } catch (caught) {
        if (active && !(caught instanceof DOMException && caught.name === "AbortError")) {
          setError(caught instanceof Error ? caught.message : "Unable to load this match.");
          setConnectionStatus("DISCONNECTED");
        }
      } finally {
        if (active) setLoading(false);
      }
    };

    void initialize();
    return () => {
      active = false;
      controller.abort();
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [applyEvents, matchId, retryToken]);

  return { state, events, loading, error, timelineError, connectionStatus, retry };
}
