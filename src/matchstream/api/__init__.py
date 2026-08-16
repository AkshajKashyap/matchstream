"""Read-only FastAPI surface for durable MatchStream projections."""

from .app import create_app

__all__ = ["create_app"]
