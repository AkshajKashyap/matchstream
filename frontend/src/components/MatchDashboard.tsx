import { EventTimeline } from "./EventTimeline";
import { MatchStats } from "./MatchStats";
import { Scoreboard } from "./Scoreboard";
import { TechnicalStatus } from "./TechnicalStatus";
import { useMatchSession } from "../hooks/useMatchSession";

export function MatchDashboard({ matchId }: { matchId: string }) {
  const session = useMatchSession(matchId);
  if (session.loading) return <main className="dashboard-status" aria-live="polite">Loading durable match state…</main>;
  if (session.error || !session.state) {
    return (
      <main className="dashboard-status" role="alert">
        <p>{session.error ?? "Match state is unavailable."}</p>
        <button type="button" onClick={session.retry}>Try again</button>
      </main>
    );
  }
  return (
    <main className="dashboard">
      <div className="connection-line" aria-live="polite">
        <span className={`status-dot status-dot--${session.connectionStatus.toLowerCase()}`} />
        <span>{session.connectionStatus}</span>
        {session.connectionStatus === "RECONNECTING" && <span className="muted">Recovering from durable state when connected.</span>}
      </div>
      <Scoreboard state={session.state} />
      <div className="dashboard__columns">
        <div><MatchStats state={session.state} /><TechnicalStatus status={session.connectionStatus} state={session.state} /></div>
        <EventTimeline events={session.events} error={session.timelineError} />
      </div>
    </main>
  );
}
