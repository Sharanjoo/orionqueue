# OrionQueue API service. This is the repo's single top-level Dockerfile,
# building the API image (the primary externally-facing service) as the
# `docker build .` default. The scheduler, worker, and frontend each have
# their own Dockerfile under deploy/docker/, since a single root Dockerfile
# can't sensibly build all four services in one file — see
# docs/architecture/system-overview.md for the full layout rationale.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api

FROM alpine:3.22 AS run
RUN addgroup -S orionqueue && adduser -S orionqueue -G orionqueue
COPY --from=build /out/api /usr/local/bin/api
USER orionqueue:orionqueue
EXPOSE 7080
ENTRYPOINT ["/usr/local/bin/api"]
