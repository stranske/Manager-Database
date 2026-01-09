import json
import logging

from etl import logging_setup


def _read_json_log(capfd):
    output = capfd.readouterr().err
    line = next(line for line in output.splitlines() if line.strip())
    return json.loads(line)


def test_configure_logging_emits_json(capfd, monkeypatch):
    logging_setup.reset_logging()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.delenv("CLOUDWATCH_LOG_GROUP", raising=False)
    logging_setup.configure_logging("etl-tests")
    logging.getLogger("etl").info("hello")
    record = _read_json_log(capfd)
    assert record["message"] == "hello"
    assert record["service"] == "etl-tests"


def test_configure_logging_adds_cloudwatch_handler(monkeypatch):
    logging_setup.reset_logging()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("CLOUDWATCH_LOG_GROUP", "test-group")
    monkeypatch.setenv("CLOUDWATCH_LOG_STREAM", "test-stream")

    events = []

    class ResourceAlreadyExistsException(Exception):
        pass

    class DummyExceptions:
        pass

    DummyExceptions.ResourceAlreadyExistsException = ResourceAlreadyExistsException

    class DummyLogsClient:
        exceptions = DummyExceptions

        def create_log_group(self, **_kwargs):
            return None

        def create_log_stream(self, **_kwargs):
            return None

        def put_log_events(self, **kwargs):
            events.extend(kwargs["logEvents"])
            return {"nextSequenceToken": "token"}

    monkeypatch.setattr(logging_setup.boto3, "client", lambda *_a, **_k: DummyLogsClient())
    logging_setup.configure_logging("etl-tests")
    logging.getLogger("etl").info("cloudwatch")

    assert events
    assert "cloudwatch" in events[0]["message"]


def test_log_outcome_uses_warning_for_empty(caplog):
    logger = logging.getLogger("etl.outcome")
    caplog.set_level(logging.INFO, logger="etl.outcome")

    logging_setup.log_outcome(logger, "done", has_data=False)

    assert any(
        record.levelno == logging.WARNING and record.message == "done" for record in caplog.records
    )

    caplog.clear()
    logging_setup.log_outcome(logger, "done", has_data=True)

    assert any(
        record.levelno == logging.INFO and record.message == "done" for record in caplog.records
    )
