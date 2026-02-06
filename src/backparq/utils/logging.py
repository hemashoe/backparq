from __future__ import annotations

import json
import logging
import sys
import threading
import uuid
from typing import Any, Optional

_context = threading.local()


def get_correlation_id() -> str:
    if not hasattr(_context, "correlation_id"):
        _context.correlation_id = str(uuid.uuid4())[:8]
    return str(_context.correlation_id)


def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    _context.correlation_id = correlation_id or str(uuid.uuid4())[:8]
    return str(_context.correlation_id)


def log_with_data(logger: logging.Logger, level: int, msg: str, **kwargs: Any) -> None:
    """Log a message with structured data."""
    if kwargs:
        pairs = [f"{k}={v}" for k, v in kwargs.items()]
        msg = f"{msg} ({', '.join(pairs)})"
    logger.log(level, msg)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "cid": get_correlation_id(),
        }
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str)


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id()
        return True


def setup_logging(
    verbosity: int = 0,
    log_format: str = "text",
    stream: Any = None,
) -> None:
    if stream is None:
        stream = sys.stderr

    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity >= 1:
        level = logging.INFO
    else:
        level = logging.WARNING

    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler(stream)
    handler.addFilter(ContextFilter())

    if log_format == "json":
        handler.setFormatter(JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S"))
    else:
        if verbosity >= 2:
            fmt = "%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s"
        elif verbosity >= 1:
            fmt = "%(asctime)s %(levelname)-8s %(message)s"
        else:
            fmt = "%(levelname)-8s %(message)s"
        handler.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))

    root.addHandler(handler)
    root.setLevel(level)

    for name in ["boto3", "botocore", "urllib3", "s3transfer"]:
        logging.getLogger(name).setLevel(logging.WARNING)

    set_correlation_id()
