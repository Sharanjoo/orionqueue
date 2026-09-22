# OrionQueue scheduler service.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/scheduler ./cmd/scheduler

FROM alpine:3.22 AS run
RUN addgroup -S orionqueue && adduser -S orionqueue -G orionqueue
COPY --from=build /out/scheduler /usr/local/bin/scheduler
USER orionqueue:orionqueue
EXPOSE 7081
ENTRYPOINT ["/usr/local/bin/scheduler"]
