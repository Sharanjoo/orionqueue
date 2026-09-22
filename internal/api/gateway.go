package api

import (
	"context"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/Sharanjoo/orionqueue/internal/api/gen/orionqueue/v1"
)

// NewGatewayMux builds a grpc-gateway *runtime.ServeMux that serves
// OrionQueue's REST API by calling directly into jobServer/workerServer
// in-process — no network hop to a separate gRPC listener. The same
// server instances also back the real gRPC server registered in
// cmd/api/main.go, so REST and gRPC clients see identical behavior; this
// is what "REST and gRPC" in the project brief means in practice, not two
// independently maintained implementations. Only WorkerService.ListWorkers
// has a REST binding (see proto/orionqueue/v1/worker_service.proto) —
// RegisterWorker/WorkerHeartbeat are gRPC-only, so they're registered on
// the gRPC server in cmd/api/main.go but never appear here.
func NewGatewayMux(ctx context.Context, jobServer pb.JobServiceServer, workerServer pb.WorkerServiceServer) (*runtime.ServeMux, error) {
	mux := runtime.NewServeMux(
		// UseProtoNames keeps REST JSON field names matching the .proto
		// (snake_case, e.g. "gpu_count") rather than protojson's default
		// camelCase — this is what the REST endpoints documented in
		// README.md and the generated OpenAPI spec (docs/api/) actually
		// send and expect.
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true},
			UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
		}),
	)
	if err := pb.RegisterJobServiceHandlerServer(ctx, mux, jobServer); err != nil {
		return nil, err
	}
	if err := pb.RegisterWorkerServiceHandlerServer(ctx, mux, workerServer); err != nil {
		return nil, err
	}
	return mux, nil
}
