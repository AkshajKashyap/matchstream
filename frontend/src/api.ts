import type { EventHistoryResponse, MatchListResponse, MatchState } from "./types";

const apiBaseUrl = (import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000").replace(/\/$/, "");
const configuredWebSocketBase = import.meta.env.VITE_WS_BASE_URL;

export const websocketBaseUrl = (configuredWebSocketBase ?? apiBaseUrl.replace(/^http/, "ws")).replace(/\/$/, "");

export class ApiError extends Error {
  constructor(message: string, public readonly status?: number) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(path: string, signal?: AbortSignal): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${apiBaseUrl}${path}`, { signal });
  } catch {
    throw new ApiError("MatchStream API is unavailable. Check that the API service is running.");
  }
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: response.statusText }));
    throw new ApiError(detail.detail ?? "API request failed", response.status);
  }
  return response.json() as Promise<T>;
}

export const api = {
  listMatches(signal?: AbortSignal): Promise<MatchListResponse> {
    return request<MatchListResponse>("/api/v1/matches?limit=100", signal);
  },
  getMatch(matchId: string, signal?: AbortSignal): Promise<MatchState> {
    return request<MatchState>(`/api/v1/matches/${encodeURIComponent(matchId)}`, signal);
  },
  getEvents(matchId: string, afterSequence: number, signal?: AbortSignal): Promise<EventHistoryResponse> {
    return request<EventHistoryResponse>(
      `/api/v1/matches/${encodeURIComponent(matchId)}/events?after_sequence=${afterSequence}&limit=50`,
      signal
    );
  },
  streamUrl(matchId: string): string {
    return `${websocketBaseUrl}/api/v1/matches/${encodeURIComponent(matchId)}/stream`;
  }
};
