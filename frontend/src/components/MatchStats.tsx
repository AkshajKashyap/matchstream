import type { MatchState } from "../types";

export function MatchStats({ state }: { state: MatchState }) {
  const counts = Object.entries(state.event_counts).sort(([left], [right]) => left.localeCompare(right));
  return (
    <section className="panel" aria-labelledby="match-stats-heading">
      <div className="section-heading"><h2 id="match-stats-heading">Available state</h2><span>Durable</span></div>
      <dl className="stats-grid">
        <div><dt>Processed events</dt><dd>{state.total_processed_events}</dd></div>
        <div><dt>Possession</dt><dd>{state.possession_team?.name ?? "Not available"}</dd></div>
        <div><dt>Latest sequence</dt><dd>{state.latest_sequence ?? "Not available"}</dd></div>
      </dl>
      {counts.length > 0 ? (
        <div className="event-counts" aria-label="Event counts">
          {counts.map(([type, count]) => <span key={type}>{type}: <b>{count}</b></span>)}
        </div>
      ) : <p className="muted">Event counts are not available yet.</p>}
    </section>
  );
}
