import type { ConnectionStatus, MatchState } from "../types";

export function TechnicalStatus({ status, state }: { status: ConnectionStatus; state: MatchState }) {
  return (
    <details className="technical-status">
      <summary>Technical status</summary>
      <dl>
        <div><dt>Connection</dt><dd>{status}</dd></div>
        <div><dt>Protocol</dt><dd>WebSocket v1</dd></div>
        <div><dt>Latest event ID</dt><dd>{state.last_event_id ?? "Not available"}</dd></div>
        <div><dt>Updated</dt><dd>{new Date(state.updated_at).toLocaleString()}</dd></div>
      </dl>
    </details>
  );
}
