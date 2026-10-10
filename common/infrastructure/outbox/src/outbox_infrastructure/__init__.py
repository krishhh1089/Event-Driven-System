"""
Outbox Infrastructure
=====================
Shared abstractions for the Transactional Outbox pattern.

Each service provides its own concrete outbox tables and publisher.
This module defines the shared interface and retry utilities.
"""
from __future__ import annotations

import asyncio
import enum
import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Outbox status — re-exported from saga_infrastructure for convenience
# ---------------------------------------------------------------------------

class OutboxStatus(str, enum.Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Base outbox publisher interface
# ---------------------------------------------------------------------------

class OutboxPublisher(ABC):
    """
    Abstract outbox publisher.

    Implementations must:
    1. Query pending outbox records (FOR UPDATE SKIP LOCKED).
    2. Publish each record to Kafka.
    3. Mark the record COMPLETED only after the broker acknowledges.
    4. Retry on transient errors with bounded exponential backoff.
    5. Mark the record FAILED after exhausting retries.
    """

    @abstractmethod
    async def publish_pending(self) -> None:
        """Find and publish all pending outbox records."""
        ...

    @abstractmethod
    async def run(self) -> None:
        """Long-running loop; call publish_pending at a configured interval."""
        ...


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

async def retry_with_backoff(
    coro_fn,
    *args,
    max_attempts: int = 5,
    base_delay_s: float = 0.5,
    max_delay_s: float = 30.0,
    jitter: bool = True,
    **kwargs,
):
    """
    Retry an async coroutine function with bounded exponential backoff + jitter.

    Raises the last exception if all attempts are exhausted.
    """
    attempt = 0
    while True:
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt >= max_attempts:
                logger.error(
                    "Exhausted %d retry attempts for %s: %s",
                    max_attempts,
                    coro_fn.__name__,
                    exc,
                )
                raise
            delay = min(base_delay_s * (2 ** (attempt - 1)), max_delay_s)
            if jitter:
                delay = delay * (0.5 + random.random() * 0.5)
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %.2fs",
                attempt,
                max_attempts,
                coro_fn.__name__,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
