"""Timestamp helpers. Everything is stored as UTC ISO 8601 text that sorts correctly."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def normalize(value: Optional[str]) -> Optional[str]:
    dt = parse_ts(value)
    return fmt_ts(dt) if dt else None


def now() -> datetime:
    return datetime.now(timezone.utc)


def ago(minutes: float = 0, days: float = 0, ref: Optional[datetime] = None) -> str:
    return fmt_ts((ref or now()) - timedelta(minutes=minutes, days=days))
