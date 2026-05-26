import logging
from collections import deque

from port.api.logging import LogForwardingHandler


def _make_handler(queue):
    handler = LogForwardingHandler(queue)
    handler.setFormatter(logging.Formatter("%(message)s"))
    return handler


def test_emit_appends_command_to_queue():
    queue = deque()
    handler = _make_handler(queue)
    record = logging.LogRecord(
        name="test", level=logging.INFO,
        pathname="", lineno=0, msg="hello world",
        args=(), exc_info=None,
    )
    handler.emit(record)
    assert len(queue) == 1
    cmd = queue[0]
    assert cmd["__type__"] == "CommandSystemLog"
    assert cmd["level"] == "info"
    assert cmd["message"] == "hello world"


def test_level_mapping():
    queue = deque()
    handler = _make_handler(queue)
    for py_level, expected in [
        (logging.DEBUG, "debug"),
        (logging.INFO, "info"),
        (logging.WARNING, "warn"),
        (logging.ERROR, "error"),
        (logging.CRITICAL, "error"),
    ]:
        queue.clear()
        record = logging.LogRecord(
            name="test", level=py_level,
            pathname="", lineno=0, msg="msg",
            args=(), exc_info=None,
        )
        handler.emit(record)
        assert queue[0]["level"] == expected, f"Expected {expected} for level {py_level}"


def test_multiple_records_all_queued():
    queue = deque()
    handler = _make_handler(queue)
    for msg in ["a", "b", "c"]:
        record = logging.LogRecord(
            name="test", level=logging.INFO,
            pathname="", lineno=0, msg=msg,
            args=(), exc_info=None,
        )
        handler.emit(record)
    assert len(queue) == 3
    assert [q["message"] for q in queue] == ["a", "b", "c"]
