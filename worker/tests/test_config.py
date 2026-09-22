import pytest

from agent import config as config_module


def test_defaults_are_valid():
    cfg = config_module.defaults()
    assert cfg.log_level in config_module.VALID_LOG_LEVELS
    assert cfg.worker_id
    assert cfg.fake_gpu is True


def test_load_applies_env_overrides(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_WORKER_ID", "worker-test-1")
    monkeypatch.setenv("ORIONQUEUE_ENVIRONMENT", "ci")
    monkeypatch.setenv("ORIONQUEUE_LOG_LEVEL", "debug")
    monkeypatch.setenv("ORIONQUEUE_FAKE_GPU", "false")

    cfg = config_module.load()

    assert cfg.worker_id == "worker-test-1"
    assert cfg.environment == "ci"
    assert cfg.log_level == "debug"
    assert cfg.fake_gpu is False


def test_load_rejects_invalid_log_level(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_LOG_LEVEL", "verbose")
    with pytest.raises(ValueError):
        config_module.load()


def test_load_rejects_empty_worker_id(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_WORKER_ID", "")
    with pytest.raises(ValueError):
        config_module.load()


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1", True),
        ("true", True),
        ("YES", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("off", False),
    ],
)
def test_fake_gpu_accepts_common_boolean_forms(monkeypatch, raw, expected):
    monkeypatch.setenv("ORIONQUEUE_FAKE_GPU", raw)
    cfg = config_module.load()
    assert cfg.fake_gpu is expected


def test_fake_gpu_rejects_unparseable_value(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_FAKE_GPU", "maybe")
    with pytest.raises(ValueError):
        config_module.load()
