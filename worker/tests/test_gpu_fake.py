import pytest

from gpu import fake


def test_discover_returns_requested_count():
    gpus = fake.discover("worker-1", count=3)
    assert len(gpus) == 3
    assert [g.device_index for g in gpus] == [0, 1, 2]


def test_discover_default_count_and_memory():
    gpus = fake.discover("worker-1")
    assert len(gpus) == fake.DEFAULT_GPU_COUNT
    assert all(g.total_memory_bytes == fake.DEFAULT_GPU_MEMORY_BYTES for g in gpus)


def test_discover_is_deterministic_for_the_same_worker_id():
    first = fake.discover("worker-abc", count=2)
    second = fake.discover("worker-abc", count=2)
    assert [g.uuid for g in first] == [g.uuid for g in second]


def test_discover_produces_different_uuids_for_different_workers():
    a = fake.discover("worker-a", count=1)
    b = fake.discover("worker-b", count=1)
    assert a[0].uuid != b[0].uuid


def test_discover_produces_different_uuids_per_device_index():
    gpus = fake.discover("worker-1", count=2)
    assert gpus[0].uuid != gpus[1].uuid


def test_discover_uuid_is_clearly_labeled_as_fake():
    gpus = fake.discover("worker-1", count=1)
    assert gpus[0].uuid.startswith("GPU-fake-")


def test_discover_zero_count_returns_empty_list():
    assert fake.discover("worker-1", count=0) == []


def test_discover_rejects_negative_count():
    with pytest.raises(ValueError):
        fake.discover("worker-1", count=-1)


def test_discover_rejects_negative_memory():
    with pytest.raises(ValueError):
        fake.discover("worker-1", memory_bytes=-1)


def test_discover_default_gpu_fields():
    gpus = fake.discover("worker-1", count=1)
    g = gpus[0]
    assert g.allocated_memory_bytes == 0
    assert g.utilization_percent == 0.0
    assert g.temperature_celsius is None
    assert g.health_state == "HEALTHY"
