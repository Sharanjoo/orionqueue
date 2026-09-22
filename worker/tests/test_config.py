import pytest

from agent import config as config_module


def test_defaults_are_valid():
    cfg = config_module.defaults()
    assert cfg.log_level in config_module.VALID_LOG_LEVELS
    assert cfg.worker_id
    assert cfg.fake_gpu is True
    assert cfg.api_grpc_addr == config_module.DEFAULT_API_GRPC_ADDR
    assert cfg.hostname
    assert cfg.cpu_capacity == config_module.DEFAULT_CPU_CAPACITY
    assert cfg.memory_capacity_bytes == config_module.DEFAULT_MEMORY_CAPACITY_BYTES
    assert cfg.fake_gpu_count > 0
    assert cfg.fake_gpu_memory_bytes > 0


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


def test_load_applies_registration_field_overrides(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_API_GRPC_ADDR", "api.internal:9080")
    monkeypatch.setenv("ORIONQUEUE_HOSTNAME", "custom-host")
    monkeypatch.setenv("ORIONQUEUE_CPU_CAPACITY", "16")
    monkeypatch.setenv("ORIONQUEUE_MEMORY_CAPACITY_BYTES", "1000")
    monkeypatch.setenv("ORIONQUEUE_FAKE_GPU_COUNT", "4")
    monkeypatch.setenv("ORIONQUEUE_FAKE_GPU_MEMORY_BYTES", "2000")

    cfg = config_module.load()

    assert cfg.api_grpc_addr == "api.internal:9080"
    assert cfg.hostname == "custom-host"
    assert cfg.cpu_capacity == 16.0
    assert cfg.memory_capacity_bytes == 1000
    assert cfg.fake_gpu_count == 4
    assert cfg.fake_gpu_memory_bytes == 2000


def test_load_rejects_negative_cpu_capacity(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_CPU_CAPACITY", "-1")
    with pytest.raises(ValueError):
        config_module.load()


def test_load_rejects_negative_memory_capacity(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_MEMORY_CAPACITY_BYTES", "-1")
    with pytest.raises(ValueError):
        config_module.load()


def test_load_rejects_negative_fake_gpu_count(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_FAKE_GPU_COUNT", "-1")
    with pytest.raises(ValueError):
        config_module.load()


def test_load_rejects_non_numeric_cpu_capacity(monkeypatch):
    monkeypatch.setenv("ORIONQUEUE_CPU_CAPACITY", "not-a-number")
    with pytest.raises(ValueError):
        config_module.load()


def test_load_rejects_empty_api_grpc_addr(monkeypatch):
    # Matches test_load_rejects_empty_worker_id's convention: an
    # explicitly-set-to-empty-string env var is invalid input, not "unset."
    monkeypatch.setenv("ORIONQUEUE_API_GRPC_ADDR", "")
    with pytest.raises(ValueError):
        config_module.load()
