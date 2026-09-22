import time
from types import SimpleNamespace

import grpc

from agent import main as main_module
from agent.config import load
from agent.grpc_client import AssignedJob, HeartbeatResult
from agent.logging_setup import configure


class _FakeRpcError(grpc.RpcError):
    """A minimal stand-in for a real grpc.RpcError, used to test
    main.py's retry/error-handling paths without a live gRPC server.
    `code`, if given, lets a test exercise main.py's
    exc.code() == grpc.StatusCode.FAILED_PRECONDITION branch specifically
    (see _start_job's report_started handling); defaults to something
    else so existing tests that never call .code() are unaffected.
    """

    def __init__(self, message: str = "", code: grpc.StatusCode = grpc.StatusCode.UNAVAILABLE):
        super().__init__(message)
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


class _FakeWorkerClient:
    """Duck-typed stand-in for agent.grpc_client.WorkerClient. main.run(),
    main._register_with_retry(), and main._start_job() only ever call
    .register(), .heartbeat(), .report_started(), .report_completed(),
    .report_failed(), and .close() on the client they're given, so a fake
    with those methods is enough — no real network involved.

    `assigned_jobs`, if given, is returned by the *first* heartbeat() call
    only (then cleared) — modeling the real server only ever handing out
    a given job assignment once, in one heartbeat response.
    """

    def __init__(
        self,
        fail_register_times: int = 0,
        heartbeat_interval_seconds: float = 1.0,
        assigned_jobs: list[AssignedJob] | None = None,
    ):
        self.register_calls = 0
        self.heartbeat_calls: list[str] = []
        self.running_job_ids_by_call: list[list[str]] = []
        self._fail_register_times = fail_register_times
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self.heartbeat_side_effect = None
        self._pending_assigned_jobs = list(assigned_jobs or [])
        self.started_calls: list[tuple[str, str]] = []
        self.completed_calls: list[tuple[str, str, int]] = []
        self.failed_calls: list[tuple[str, str, str]] = []
        self.stopped_calls: list[tuple[str, str]] = []
        # Called (if set) with job_id from inside report_started, before
        # it's recorded — lets a test make report_started raise, e.g. to
        # simulate the server rejecting it (see FAILED_PRECONDITION tests).
        self.report_started_side_effect = None

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
        self.running_job_ids_by_call.append(list(running_job_ids))
        if self.heartbeat_side_effect is not None:
            self.heartbeat_side_effect(worker_id)
        jobs, self._pending_assigned_jobs = self._pending_assigned_jobs, []
        return HeartbeatResult(assigned_jobs=jobs, stop_job_ids=[])

    def report_started(self, job_id, worker_id):
        if self.report_started_side_effect is not None:
            self.report_started_side_effect(job_id)
        self.started_calls.append((job_id, worker_id))

    def report_completed(self, job_id, worker_id, exit_code=0):
        self.completed_calls.append((job_id, worker_id, exit_code))

    def report_failed(self, job_id, worker_id, failure_reason):
        self.failed_calls.append((job_id, worker_id, failure_reason))

    def report_stopped(self, job_id, worker_id):
        self.stopped_calls.append((job_id, worker_id))

    def close(self):
        pass


class _StickyAssignmentClient(_FakeWorkerClient):
    """A fake client that keeps re-offering the same job assignment for
    `offer_for_calls` heartbeats (modeling a job that stays in
    assigned_jobs across more than one heartbeat, e.g. if a heartbeat
    response were ever redelivered) and stops the run loop after
    `stop_after_calls` heartbeats. Used to test that main.py does not
    start a job a second time while it's already running, and that
    running_job_ids reported on later heartbeats reflect it.
    """

    def __init__(
        self,
        job: AssignedJob,
        shutdown_state: dict,
        offer_for_calls: int,
        stop_after_calls: int,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._job = job
        self._shutdown_state = shutdown_state
        self._offer_for_calls = offer_for_calls
        self._stop_after_calls = stop_after_calls

    def heartbeat(self, worker_id, gpus, running_job_ids):
        self.heartbeat_calls.append(worker_id)
        self.running_job_ids_by_call.append(list(running_job_ids))
        call_n = len(self.heartbeat_calls)
        if call_n >= self._stop_after_calls:
            self._shutdown_state["value"] = True
        jobs = [self._job] if call_n <= self._offer_for_calls else []
        return HeartbeatResult(assigned_jobs=jobs, stop_job_ids=[])


class _StopSignalClient(_FakeWorkerClient):
    """Offers `job` once (on the first heartbeat), then delivers a stop
    signal for it (stop_job_ids) starting from the `stop_from_call`-th
    heartbeat onward, stopping the run loop after `stop_after_calls`
    heartbeats. Used to test that a stop signal delivered mid-execution
    actually interrupts a running job's executor thread — see
    executors.fake.run()'s should_stop hook and main.py's cancel_events.
    """

    def __init__(
        self,
        job: AssignedJob,
        shutdown_state: dict,
        stop_from_call: int,
        stop_after_calls: int,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._job = job
        self._shutdown_state = shutdown_state
        self._stop_from_call = stop_from_call
        self._stop_after_calls = stop_after_calls

    def heartbeat(self, worker_id, gpus, running_job_ids):
        self.heartbeat_calls.append(worker_id)
        self.running_job_ids_by_call.append(list(running_job_ids))
        call_n = len(self.heartbeat_calls)
        if call_n >= self._stop_after_calls:
            self._shutdown_state["value"] = True
        jobs = [self._job] if call_n == 1 else []
        stop_ids = [self._job.job_id] if call_n >= self._stop_from_call else []
        return HeartbeatResult(assigned_jobs=jobs, stop_job_ids=stop_ids)


def _raise_failed_precondition(job_id: str) -> None:
    raise _FakeRpcError("job is not SCHEDULED", code=grpc.StatusCode.FAILED_PRECONDITION)


def _test_logger():
    return configure("orionqueue-worker-test", "test", "info")


def _wait_until(predicate, timeout_seconds: float = 2.0) -> bool:
    """Polls predicate() until it returns truthy or timeout_seconds
    elapses. Needed because job execution happens on a background daemon
    thread (see main._start_job) — run() returning is not enough to
    guarantee a fast fake job has finished executing yet.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


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
        return HeartbeatResult(assigned_jobs=[], stop_job_ids=[])

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


def test_run_executes_assigned_job_and_reports_completed(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)

    job = AssignedJob(job_id="job-1", command=["--steps=1", "--step-seconds=0"], timeout_seconds=0)

    client = _StickyAssignmentClient(
        job, state, offer_for_calls=1, stop_after_calls=3, heartbeat_interval_seconds=0.05
    )

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    assert _wait_until(lambda: client.completed_calls)
    assert client.started_calls == [("job-1", "worker-fake-1")]
    assert client.completed_calls == [("job-1", "worker-fake-1", 0)]
    assert client.failed_calls == []


def test_run_executes_assigned_job_and_reports_failure(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)

    job = AssignedJob(
        job_id="job-2",
        command=["--steps=3", "--step-seconds=0", "--fail-at-step=1"],
        timeout_seconds=0,
    )

    client = _StickyAssignmentClient(
        job, state, offer_for_calls=1, stop_after_calls=3, heartbeat_interval_seconds=0.05
    )

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    assert _wait_until(lambda: client.failed_calls)
    assert client.started_calls == [("job-2", "worker-fake-1")]
    assert client.completed_calls == []
    assert len(client.failed_calls) == 1
    failed_job_id, failed_worker_id, reason = client.failed_calls[0]
    assert failed_job_id == "job-2"
    assert failed_worker_id == "worker-fake-1"
    assert "step 1" in reason


def test_run_does_not_start_an_already_running_job_twice(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)
    # run() gets its heartbeat interval from _register_with_retry, which
    # floors whatever the client returns to 1.0s (see main.py) — bypass
    # that here so the loop actually ticks at 0.05s and this test doesn't
    # need a multi-second job to outlast it.
    monkeypatch.setattr(
        main_module,
        "_register_with_retry",
        lambda cfg, logger, client, gpus, state: ("worker-fake-1", 0.05),
    )

    # A slow job (step_seconds=1.0) offered across two heartbeats: it
    # cannot possibly finish within the ~0.1-0.15s the loop takes to
    # reach its third (stopping) heartbeat, so if main.py were re-starting
    # it on every heartbeat where it appears, started_calls would have
    # two entries instead of one.
    job = AssignedJob(
        job_id="job-3", command=["--steps=1", "--step-seconds=1.0"], timeout_seconds=0
    )

    client = _StickyAssignmentClient(
        job, state, offer_for_calls=2, stop_after_calls=3, heartbeat_interval_seconds=0.05
    )

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    assert client.started_calls == [("job-3", "worker-fake-1")]
    assert client.completed_calls == []  # still running when run() returned


def test_run_includes_started_job_in_later_running_job_ids(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)
    monkeypatch.setattr(
        main_module,
        "_register_with_retry",
        lambda cfg, logger, client, gpus, state: ("worker-fake-1", 0.05),
    )

    job = AssignedJob(
        job_id="job-4", command=["--steps=1", "--step-seconds=1.0"], timeout_seconds=0
    )

    client = _StickyAssignmentClient(
        job, state, offer_for_calls=1, stop_after_calls=3, heartbeat_interval_seconds=0.05
    )

    main_module.run(load(), _test_logger(), client, [])

    assert client.running_job_ids_by_call[0] == []  # job not yet assigned before first heartbeat
    assert client.running_job_ids_by_call[1] == ["job-4"]  # started right after first heartbeat
    assert client.running_job_ids_by_call[2] == ["job-4"]  # still running


def test_run_stops_a_running_job_when_stop_signal_is_delivered(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)
    monkeypatch.setattr(
        main_module,
        "_register_with_retry",
        lambda cfg, logger, client, gpus, state: ("worker-fake-1", 0.05),
    )

    # A job that would run for ~5s unless actually interrupted by the
    # stop signal well before then.
    job = AssignedJob(
        job_id="job-5", command=["--steps=100", "--step-seconds=0.05"], timeout_seconds=0
    )
    client = _StopSignalClient(
        job, state, stop_from_call=2, stop_after_calls=6, heartbeat_interval_seconds=0.05
    )

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    assert _wait_until(lambda: client.stopped_calls)
    assert client.stopped_calls == [("job-5", "worker-fake-1")]
    assert client.completed_calls == []
    assert client.failed_calls == []


def test_run_does_not_execute_a_job_whose_report_started_is_rejected(monkeypatch):
    state = {"value": False}
    monkeypatch.setattr(main_module, "_install_signal_handlers", lambda logger: state)

    # A job that would take a long time if it were ever actually executed
    # — proving it never ran is the point of this test.
    job = AssignedJob(
        job_id="job-6", command=["--steps=100", "--step-seconds=1"], timeout_seconds=0
    )
    client = _StickyAssignmentClient(
        job, state, offer_for_calls=1, stop_after_calls=2, heartbeat_interval_seconds=0.05
    )
    client.report_started_side_effect = _raise_failed_precondition

    exit_code = main_module.run(load(), _test_logger(), client, [])

    assert exit_code == 0
    # Nothing to poll for (a job that's never executed reports nothing) —
    # a short fixed wait gives the background thread a chance to run its
    # early-return path before asserting it did nothing further.
    time.sleep(0.2)
    assert client.completed_calls == []
    assert client.failed_calls == []
    assert client.stopped_calls == []
