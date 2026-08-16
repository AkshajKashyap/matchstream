import { useEffect, useState } from "react";

import { api } from "./api";
import { MatchDashboard } from "./components/MatchDashboard";
import { MatchList } from "./components/MatchList";
import type { MatchSummary } from "./types";

export function App() {
  const [matches, setMatches] = useState<MatchSummary[]>([]);
  const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshToken, setRefreshToken] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    void api.listMatches(controller.signal).then((response) => {
      setMatches(response.matches);
      setSelectedMatchId((current) => response.matches.some((match) => match.match_id === current) ? current : (response.matches[0]?.match_id ?? null));
    }).catch((caught) => {
      if (!(caught instanceof DOMException && caught.name === "AbortError")) setError(caught instanceof Error ? caught.message : "Unable to load matches.");
    }).finally(() => setLoading(false));
    return () => controller.abort();
  }, [refreshToken]);

  return (
    <div className="app-shell">
      <header className="site-header"><a href="/" className="wordmark">Match<span>Stream</span></a><p>Real-time football event streaming and match analytics</p></header>
      {loading ? <main className="dashboard-status" aria-live="polite">Loading matches…</main> : error ? (
        <main className="dashboard-status" role="alert"><p>{error}</p><button type="button" onClick={() => setRefreshToken((value) => value + 1)}>Try again</button></main>
      ) : matches.length === 0 ? <main className="dashboard-status">No matches are available yet.</main> : (
        <div className="workspace">
          <MatchList matches={matches} selectedMatchId={selectedMatchId} onSelect={setSelectedMatchId} />
          {selectedMatchId && <MatchDashboard matchId={selectedMatchId} />}
        </div>
      )}
    </div>
  );
}
