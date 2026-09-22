from types import SimpleNamespace

import grpc

from agent import main as main_module
from agent.config import load
from agent.logging_setup import configure


class _FakeRpcError(grpc.RpcError):
    """A minimal stand-in for a real grpc.RpcError, used to test
    main.py's retry/error-handling paths without a live gRPC server.
    """


class _FakeWorkerClient:
    """Duck-typed stand-in for agent.grpc_client.WorkerClient. main.run()
    and main._register_with_retry() only ever call .register(),
    .heartbeat(), and .close() on the client they're given, so a fake with
    the same three methods is enough — no real network involved.
    """

    def __init__(self, fail_register_times: int = 0, heartbeat_interval_seconds: float = 1.0):
        self.register_calls = 0
        self.heartbeat_calls: list[str] = []
        self._fail_register_times = fail_register_times
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self.heartbeat_side_effect = None

    def register(self, **kwargs):
        self.register_calls += 1
        if self.register_calls <= self._fail_register_times:
            raise _FakeRpcError("registration temporarily unavailable")
        return SimpleNamespace(
            worker_id="worker-fake-1",
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )

    def heartbeat(self, worker_id, gpus, running_job_ids):
        self.heartbeat_calls.append(worker_id)
        if self.heartbeat_side_effect is not None:
            self.heartbeat_side_effect(worker_id)

    def close(self):
        pass


def _test_logger():
    return configure("orionqueue-worker-test", "test", "info")


def test_register_with_retry_succeeds_immediately_on_first_try():
    client = _FakeWorkerClient()
    state = {"value": False}
    worker_id, interval = main_module._register_with_retry(
        load(), _test_logger(), client, [], state
    )
    assert worker_id == "worker-fake-1"
    assert interval == 1.0
    assert client.register_calls == 1


def test_register_with_retry_retries_after_transient_failures(monkeypatch):
    monkeypatch.setattr(main_module, "_REGISTER_INITIAL_BACKOFF_SECONDS", 0.01)
    monkeypatch.setattr(main_module, "_REGISTER_MAX_BACKOFF_SECONDS", 0.02)

    client = _FakeWorkerClient(fail_register_times=2)
    state = {"value": False}
    worker_id, interval = main_module._register_with_retry(
        load(), _test_logger(), client, [], state
    )
    assert worker_id == "worker-fake-1"
    assert client.register_calls == 3


def test_register_with_retry_stops_immediately_if_shutdown_already_requested():
    client = _FakeWorkerClient(fail_register_times=999)
    state = {"value": True}
    worker_id, interval = main_module._register_with_retry(
        load(), _test_logger(), client, [], state
    )
    assert worker_id is None
    assert interval is None
    assert client.register_calls == 0


def test_run_registers_and_heartbeats_then_stops_on_shutdown(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)

    client = _FakeWorkerClient(heartbeat_interval_seconds=0.05)

    def stop_after_one_heartbeat(worker_id):
        state["value"] = True

    client.heartbeat_side_effect = stop_after_one_heartbeat

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    assert client.register_calls == 1
    assert client.heartbeat_calls == ["worker-fake-1"]


def test_run_returns_immediately_if_shutdown_requested_before_registration(monkeypatch):
    state = {"value": True}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)

    client = _FakeWorkerClient()
    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    assert client.register_calls == 0
    assert client.heartbeat_calls == []


def test_run_continues_after_a_heartbeat_failure(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)

    client = _FakeWorkerClient(heartbeat_interval_seconds=0.05)
    attempts: list[str] = []

    def heartbeat_with_flakiness(worker_id, gpus, running_job_ids):
        attempts.append(worker_id)
        if len(attempts) == 1:
            raise _FakeRpcError("transient network error")
        state["value"] = True

    client.heartbeat = heartbeat_with_flakiness

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    # Both heartbeat attempts happened (one raised, one succeeded) — a
    # single failed heartbeat must not crash the agent or stop the loop.
    assert len(attempts) == 2


def test_main_returns_error_and_logs_on_invalid_config(monkeypatch, capsys):
    monkeypatch.setenv("ORIONQUEUE_LOG_LEVEL", "not-a-level")

    exit_code = main_module.main([])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "invalid configuration" in err
    assert "not-a-level" in err
