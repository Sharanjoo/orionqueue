import pytest

from executors import fake


def test_parse_command_defaults_on_empty_command():
    cfg = fake.parse_command([])
    assert cfg.steps == fake.DEFAULT_STEPS
    assert cfg.step_seconds == fake.DEFAULT_STEP_SECONDS
    assert cfg.fail_at_step is None


def test_parse_command_reads_all_flags():
    cfg = fake.parse_command(["--steps=3", "--step-seconds=0.1", "--fail-at-step=2"])
    assert cfg.steps == 3
    assert cfg.step_seconds == 0.1
    assert cfg.fail_at_step == 2


def test_parse_command_ignores_unrecognized_entries():
    cfg = fake.parse_command(["python", "train.py", "--steps=2"])
    assert cfg.steps == 2


def test_parse_command_rejects_invalid_steps():
    with pytest.raises(ValueError):
        fake.parse_command(["--steps=0"])
    with pytest.raises(ValueError):
        fake.parse_command(["--steps=not-a-number"])


def test_parse_command_rejects_negative_step_seconds():
    with pytest.raises(ValueError):
        fake.parse_command(["--step-seconds=-1"])


def test_run_succeeds_with_no_failure_configured():
    outcome = fake.run(["--steps=3", "--step-seconds=0"])
    assert outcome.success is True
    assert outcome.steps_completed == 3
    assert outcome.failure_reason == ""
    assert outcome.stopped is False


def test_run_fails_at_the_configured_step():
    outcome = fake.run(["--steps=5", "--step-seconds=0", "--fail-at-step=3"])
    assert outcome.success is False
    assert outcome.steps_completed == 2  # steps 1 and 2 completed; failed entering step 3
    assert "step 3" in outcome.failure_reason
    assert outcome.stopped is False  # a genuine failure, not a cooperative stop


def test_run_is_deterministic_across_repeated_calls():
    first = fake.run(["--steps=4", "--step-seconds=0", "--fail-at-step=2"])
    second = fake.run(["--steps=4", "--step-seconds=0", "--fail-at-step=2"])
    assert first == second


def test_run_calls_on_step_for_each_completed_step():
    calls = []
    fake.run(
        ["--steps=3", "--step-seconds=0"], on_step=lambda step, total: calls.append((step, total))
    )
    assert calls == [(1, 3), (2, 3), (3, 3)]


def test_run_does_not_call_on_step_for_the_failed_step():
    calls = []
    fake.run(
        ["--steps=3", "--step-seconds=0", "--fail-at-step=2"],
        on_step=lambda step, total: calls.append((step, total)),
    )
    assert calls == [(1, 3)]


def test_run_stops_early_when_should_stop_returns_true():
    outcome = fake.run(["--steps=5", "--step-seconds=0"], should_stop=lambda: True)
    assert outcome.success is False
    assert outcome.steps_completed == 0
    assert "stopped" in outcome.failure_reason
    assert outcome.stopped is True


def test_run_should_stop_is_polled_between_steps_not_just_once():
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 2  # let 2 steps run, then stop before the 3rd

    outcome = fake.run(["--steps=5", "--step-seconds=0"], should_stop=should_stop)
    assert outcome.success is False
    assert outcome.stopped is True
    assert outcome.steps_completed == 2


def test_run_respects_timeout():
    outcome = fake.run(["--steps=100", "--step-seconds=0.05"], timeout_seconds=0.1)
    assert outcome.success is False
    assert "timed out" in outcome.failure_reason
    assert outcome.steps_completed < 100


def test_run_zero_timeout_means_no_timeout():
    outcome = fake.run(["--steps=2", "--step-seconds=0.01"], timeout_seconds=0)
    assert outcome.success is True
