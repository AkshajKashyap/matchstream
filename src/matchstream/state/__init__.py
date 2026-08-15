"""Deterministic, process-local match-state projections."""

from .models import MatchState, StateValidationError
from .projector import MatchProjector, ProjectionUpdate
from .transition import (
    ClockRegressionError,
    DuplicateEventError,
    MatchMismatchError,
    MatchTransitionError,
    ScoreReconstructionError,
    SequenceGapError,
    StaleEventError,
    apply_event,
    is_goal,
)

__all__ = [
    "ClockRegressionError",
    "DuplicateEventError",
    "DurableMatchProjector",
    "MatchMismatchError",
    "MatchProjector",
    "MatchState",
    "MatchTransitionError",
    "ProjectionUpdate",
    "ScoreReconstructionError",
    "SequenceGapError",
    "StaleEventError",
    "StateValidationError",
    "apply_event",
    "is_goal",
]
from .durable import DurableMatchProjector
