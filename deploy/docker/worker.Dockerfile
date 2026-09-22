# OrionQueue worker agent (Python). This image runs the fake-GPU simulator
# by default (ORIONQUEUE_FAKE_GPU=1) — see docs/adr/0003-fake-vs-real-gpu.md.
# A CUDA-enabled variant for real-GPU mode is added in Phase 9.
FROM python:3.12-slim AS build
WORKDIR /src
COPY worker/pyproject.toml worker/README.md ./
COPY worker/agent ./agent
COPY worker/gpu ./gpu
COPY worker/gen ./gen
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim AS run
RUN useradd --system --create-home orionqueue
COPY --from=build /install /usr/local
USER orionqueue
ENV ORIONQUEUE_FAKE_GPU=1
ENTRYPOINT ["python", "-m", "agent.main"]
