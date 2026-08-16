import { formatClock } from "./MatchList";
import type { MatchState } from "../types";

interface ScoreboardProps {
  state: MatchState;
}

function teamName(team: MatchState["home_team"]): string {
  return team?.name ?? "Unknown team";
}

function scoreFor(state: MatchState, team: MatchState["home_team"]): number {
  return team ? (state.score[team.id] ?? 0) : 0;
}

export function Scoreboard({ state }: ScoreboardProps) {
  return (
    <section className="scoreboard" aria-label="Current score">
      <p className="eyebrow">Live match state</p>
      <div className="scoreboard__teams">
        <div><span>{teamName(state.home_team)}</span><strong>{scoreFor(state, state.home_team)}</strong></div>
        <div className="scoreboard__clock"><strong>{formatClock(state.match_clock_seconds)}</strong><span>Period {state.period}</span></div>
        <div><strong>{scoreFor(state, state.away_team)}</strong><span>{teamName(state.away_team)}</span></div>
      </div>
    </section>
  );
}
