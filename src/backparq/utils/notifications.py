"""Notification utilities."""

from __future__ import annotations

import logging
from typing import Any, Optional

try:
    import httpx
except ImportError:
    httpx = None

from backparq.config import NotificationConfig
from backparq.utils.logging import log_with_data

logger = logging.getLogger(__name__)


def send_notification(
    config: Optional[NotificationConfig],
    event: str,
    payload: dict[str, Any],
) -> None:
    """
    Send a notification webhook.

    Args:
        config: Notification configuration object.
        event: Event name (e.g., "archive_started", "archive_success", "archive_failed").
        payload: Data to send in the webhook body.
    """
    if not config or not config.enabled or not config.urls:
        return

    if not httpx:
        logger.warning(
            "Notification enabled but 'httpx' not installed. Run 'pip install backparq[notifications]'."
        )
        return

    # Check if we should send based on event type
    if event == "archive_success" and not config.on_success:
        return
    if event == "archive_failed" and not config.on_failure:
        return

    # Prepare standard payload
    message = {
        "event": event,
        "timestamp": payload.get("timestamp"),
        "run_id": payload.get("run_id"),
        "data": payload,
    }

    for url in config.urls:
        try:
            response = httpx.post(url, json=message, timeout=10.0)
            response.raise_for_status()
            log_with_data(logger, logging.DEBUG, "Sent notification", url=url, event=event)
        except Exception as e:
            log_with_data(
                logger, logging.ERROR, "Failed to send notification", url=url, error=str(e)
            )
