import type { MatchSummary } from "../types";

interface MatchListProps {
  matches: MatchSummary[];
  selectedMatchId: string | null;
  onSelect(matchId: string): void;
}

function teamName(team: MatchSummary["home_team"]): string {
  return team?.name ?? "Unknown team";
}

function scoreFor(score: Record<string, number>, team: MatchSummary["home_team"]): number {
  return team ? (score[team.id] ?? 0) : 0;
}

export function MatchList({ matches, selectedMatchId, onSelect }: MatchListProps) {
  return (
    <nav className="match-list" aria-label="Available matches">
      <div className="section-heading">
        <p className="eyebrow">Matches</p>
        <span>{matches.length}</span>
      </div>
      <div className="match-list__items">
        {matches.map((match) => {
          const home = teamName(match.home_team);
          const away = teamName(match.away_team);
          return (
            <button
              className="match-card"
              key={match.match_id}
              type="button"
              aria-pressed={match.match_id === selectedMatchId}
              onClick={() => onSelect(match.match_id)}
            >
              <span className="match-card__teams">{home} <b>{scoreFor(match.score, match.home_team)}</b></span>
              <span className="match-card__teams">{away} <b>{scoreFor(match.score, match.away_team)}</b></span>
              <span className="match-card__meta">Period {match.period} · {formatClock(match.match_clock_seconds)}</span>
              <span className="match-card__identifier">ID {match.match_id}</span>
            </button>
          );
        })}
      </div>
    </nav>
  );
}

export function formatClock(seconds: number): string {
  const totalSeconds = Math.floor(seconds);
  const minutes = Math.floor(totalSeconds / 60);
  const remainingSeconds = totalSeconds % 60;
  return `${minutes}:${remainingSeconds.toString().padStart(2, "0")}`;
}
