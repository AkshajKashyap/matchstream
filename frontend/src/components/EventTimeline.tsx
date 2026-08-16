import { formatClock } from "./MatchList";
import type { CanonicalEvent } from "../types";

interface EventTimelineProps {
  events: CanonicalEvent[];
  error: string | null;
}

export function EventTimeline({ events, error }: EventTimelineProps) {
  return (
    <section className="panel timeline" aria-labelledby="timeline-heading">
      <div className="section-heading"><h2 id="timeline-heading">Event timeline</h2><span>Last {events.length}</span></div>
      {error && <p className="inline-error" role="status">{error}</p>}
      {events.length === 0 ? <p className="muted">No recorded events are available for this match.</p> : (
        <ol>
          {events.map((event) => (
            <li key={event.event_id}>
              <time>#{event.sequence} · {formatClock(event.match_clock_seconds)}</time>
              <div><strong>{event.event_type}</strong><span>{[event.team?.name, event.player?.name].filter(Boolean).join(" · ") || "No team or player recorded"}</span></div>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
