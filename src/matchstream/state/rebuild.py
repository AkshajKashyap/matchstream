"""Pure offline reconstruction of a match state from canonical history."""

from __future__ import annotations

from collections.abc import Iterable

from matchstream.models import CanonicalEvent, event_order_key

from .models import MatchState
from .transition import apply_event


def rebuild_state(match_id: str, events: Iterable[CanonicalEvent]) -> MatchState:
    """Deterministically rebuild one match and revalidate every transition."""

    state = MatchState.initial(match_id)
    for event in sorted(events, key=event_order_key):
        state = apply_event(state, event)
    return state
