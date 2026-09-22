from agent.grpc_client import _gpu_to_proto
from gpu.fake import GPU


def test_gpu_to_proto_maps_all_fields():
    g = GPU(
        device_index=1,
        uuid="GPU-fake-abc",
        total_memory_bytes=1000,
        allocated_memory_bytes=200,
        utilization_percent=42.5,
        temperature_celsius=65.0,
        mig_profile="1g.5gb",
        health_state="HEALTHY",
    )
    pb = _gpu_to_proto(g)

    assert pb.device_index == 1
    assert pb.uuid == "GPU-fake-abc"
    assert pb.total_memory_bytes == 1000
    assert pb.allocated_memory_bytes == 200
    assert pb.utilization_percent == 42.5
    assert pb.temperature_celsius == 65.0
    assert pb.mig_profile == "1g.5gb"


def test_gpu_to_proto_omits_temperature_when_none():
    g = GPU(device_index=0, uuid="GPU-fake-1", total_memory_bytes=1000, temperature_celsius=None)
    pb = _gpu_to_proto(g)
    assert not pb.HasField("temperature_celsius")


def test_gpu_to_proto_maps_unhealthy_state():
    g = GPU(device_index=0, uuid="GPU-fake-1", total_memory_bytes=1000, health_state="UNHEALTHY")
    pb = _gpu_to_proto(g)
    from orionqueue.v1 import worker_pb2

    assert pb.health_state == worker_pb2.GPU_HEALTH_STATE_UNHEALTHY
