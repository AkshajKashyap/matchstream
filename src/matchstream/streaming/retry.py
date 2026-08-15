"""Small explicit failure classification and bounded retry policy."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from matchstream.state import MatchTransitionError
from matchstream.storage import DurableStorageError

from .dlq import FailureClassification
from .redpanda import BrokerTransportError


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded in-process retry policy; attempts include the first execution."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.0
    max_delay_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays must be non-negative")

    def delay_for_retry(self, completed_attempts: int) -> float:
        """Return a capped exponential delay before the next attempt."""

        return min(self.base_delay_seconds * (2 ** (completed_attempts - 1)), self.max_delay_seconds)


class RetryExhaustedError(RuntimeError):
    """Raised when a retriable/unknown failure exceeds the bounded retry policy."""


@dataclass(frozen=True, slots=True)
class ProcessingFailure:
    """A classified failure after all in-process attempts were used."""

    error: Exception
    classification: FailureClassification
    attempt_count: int


def classify_failure(error: Exception) -> FailureClassification:
    """Classify only deterministic transition failures as quarantine candidates."""

    if isinstance(error, MatchTransitionError):
        return FailureClassification.POISON
    if isinstance(error, (DurableStorageError, BrokerTransportError)):
        return FailureClassification.RETRIABLE
    return FailureClassification.RETRIABLE


def attempt_processing(
    operation: Callable[[], object],
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
) -> object | ProcessingFailure:
    """Run a bounded operation; only poison failures return for quarantining."""

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation()
        except Exception as error:
            classification = classify_failure(error)
            if attempt == policy.max_attempts:
                if classification is FailureClassification.POISON:
                    return ProcessingFailure(error, classification, attempt)
                raise RetryExhaustedError(
                    f"retriable processing failed after {attempt} attempts: {error}"
                ) from error
            delay = policy.delay_for_retry(attempt)
            if delay:
                sleep(delay)
    raise AssertionError("retry loop ended without result")

