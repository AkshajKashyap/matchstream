export interface ApiEntity {
  id: string;
  name: string;
}

export interface MatchSummary {
  match_id: string;
  period: number;
  match_clock_seconds: number;
  latest_sequence: number | null;
  home_team: ApiEntity | null;
  away_team: ApiEntity | null;
  score: Record<string, number>;
  updated_at: string;
}

export interface MatchState extends MatchSummary {
  total_processed_events: number;
  possession_team: ApiEntity | null;
  event_counts: Record<string, number>;
  last_event_id: string | null;
}

export interface CanonicalEvent {
  event_id: string;
  match_id: string;
  sequence: number;
  period: number;
  match_clock_seconds: number;
  event_type: string;
  team: ApiEntity | null;
  player: ApiEntity | null;
  location: number[] | null;
  possession_id: string | null;
  possession_team: ApiEntity | null;
  metadata: Record<string, unknown>;
  source: string;
  source_event_id: string;
}

export interface MatchListResponse {
  matches: MatchSummary[];
  next_after_match_id: string | null;
}

export interface EventHistoryResponse {
  events: CanonicalEvent[];
  next_after_sequence: number | null;
}

export interface WebSocketSnapshot {
  type: "snapshot";
  protocol_version: 1;
  state: MatchState;
}

export interface WebSocketStateUpdate {
  type: "state_update";
  protocol_version: 1;
  state: MatchState;
}

export type StreamMessage = WebSocketSnapshot | WebSocketStateUpdate;
export type ConnectionStatus = "CONNECTING" | "LIVE" | "RECONNECTING" | "DISCONNECTED";
