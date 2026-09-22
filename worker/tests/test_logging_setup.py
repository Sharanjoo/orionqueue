import json

from agent.logging_setup import configure


def test_configure_emits_json_with_expected_fields(capsys):
    logger = configure("orionqueue-worker", "test-env", "info")
    logger.info("hello", extra={"fields": {"worker_id": "w1"}})

    captured = capsys.readouterr().out.strip()
    payload = json.loads(captured)

    assert payload["msg"] == "hello"
    assert payload["service"] == "orionqueue-worker"
    assert payload["environment"] == "test-env"
    assert payload["level"] == "info"
    assert payload["worker_id"] == "w1"


def test_configure_respects_log_level_filtering(capsys):
    logger = configure("orionqueue-worker", "test-env", "warning")
    logger.info("should not appear")
    logger.warning("should appear")

    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["msg"] == "should appear"


def test_configure_does_not_duplicate_handlers_on_repeat_calls(capsys):
    configure("svc", "env", "info")
    logger = configure("svc", "env", "info")
    logger.info("once")

    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
