# core/utils.py

import logging
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def to_local_dt(utc_iso: str, tz_name: str = "America/Los_Angeles") -> datetime:
    """
    Parse a UTC ISO-8601 string and convert to the given local timezone.
    Used for display-layer formatting only — all stored timestamps remain UTC.

    Example:
        to_local_dt("2026-04-23T18:00:00+00:00").strftime("%Y-%m-%d %I:%M %p %Z")
        → "2026-04-23 11:00 AM PDT"
    """
    dt = datetime.fromisoformat(utc_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo(tz_name))

def retry_with_backoff(max_retries: int = 3, base_delay: float = 2.0) -> Callable:
    """
    Decorator that retries a function with exponential backoff.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        raise
                    sleep_time = base_delay * (2 ** (retries - 1))
                    logger.warning(
                        f"Retrying {func.__name__} in {sleep_time}s due to error: {e}"
                    )
                    time.sleep(sleep_time)
        return wrapper
    return decorator
