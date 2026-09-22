package config

import "testing"

func TestDefaultsAreValid(t *testing.T) {
	for _, service := range []string{"orionqueue-api", "orionqueue-scheduler", "anything-else"} {
		cfg := Defaults(service)
		if err := cfg.Validate(); err != nil {
			t.Errorf("Defaults(%q) produced an invalid config: %v", service, err)
		}
	}
}

func TestDefaultHTTPAddrIsPerService(t *testing.T) {
	api := Defaults("orionqueue-api")
	scheduler := Defaults("orionqueue-scheduler")
	if api.HTTPAddr == scheduler.HTTPAddr {
		t.Fatalf("expected orionqueue-api and orionqueue-scheduler to default to different addresses, both got %q", api.HTTPAddr)
	}
}

func TestLoadAppliesEnvOverrides(t *testing.T) {
	t.Setenv("ORIONQUEUE_ENVIRONMENT", "ci")
	t.Setenv("ORIONQUEUE_HTTP_ADDR", ":9090")
	t.Setenv("ORIONQUEUE_GRPC_ADDR", ":9091")
	t.Setenv("ORIONQUEUE_LOG_LEVEL", "debug")

	cfg, err := Load("orionqueue-api")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.Environment != "ci" {
		t.Errorf("Environment = %q, want %q", cfg.Environment, "ci")
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":9090")
	}
	if cfg.GRPCAddr != ":9091" {
		t.Errorf("GRPCAddr = %q, want %q", cfg.GRPCAddr, ":9091")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
}

func TestDefaultGRPCAddrIsPerServiceAndDiffersFromHTTPAddr(t *testing.T) {
	api := Defaults("orionqueue-api")
	scheduler := Defaults("orionqueue-scheduler")
	if api.GRPCAddr == scheduler.GRPCAddr {
		t.Fatalf("expected orionqueue-api and orionqueue-scheduler to default to different gRPC addresses, both got %q", api.GRPCAddr)
	}
	if api.GRPCAddr == api.HTTPAddr {
		t.Fatalf("expected GRPCAddr and HTTPAddr to differ, both got %q", api.GRPCAddr)
	}
}

func TestLoadRejectsMalformedGRPCAddr(t *testing.T) {
	t.Setenv("ORIONQUEUE_GRPC_ADDR", "not-an-address")
	if _, err := Load("orionqueue-api"); err == nil {
		t.Fatal("expected Load to reject a malformed ORIONQUEUE_GRPC_ADDR")
	}
}

func TestDefaultDatabaseURLIsSetForClusterServicesOnly(t *testing.T) {
	api := Defaults("orionqueue-api")
	if api.DatabaseURL == "" {
		t.Error("expected orionqueue-api to have a non-empty default DatabaseURL")
	}
	// orionqueue-scheduler also needs PostgreSQL as of Phase 5 (to fetch
	// active jobs/workers and persist scheduling decisions).
	scheduler := Defaults("orionqueue-scheduler")
	if scheduler.DatabaseURL == "" {
		t.Error("expected orionqueue-scheduler to have a non-empty default DatabaseURL")
	}
	other := Defaults("some-future-service")
	if other.DatabaseURL != "" {
		t.Errorf("expected an unrecognized service to have an empty default DatabaseURL, got %q", other.DatabaseURL)
	}
}

func TestLoadAppliesDatabaseURLOverride(t *testing.T) {
	t.Setenv("ORIONQUEUE_DATABASE_URL", "postgres://u:p@example.invalid:5432/db")
	cfg, err := Load("orionqueue-api")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.DatabaseURL != "postgres://u:p@example.invalid:5432/db" {
		t.Errorf("DatabaseURL = %q, want the overridden value", cfg.DatabaseURL)
	}
}

func TestLoadRejectsEmptyDatabaseURLForAPI(t *testing.T) {
	// Force DatabaseURL empty in a way env-override can't: Validate must
	// reject it directly since orionqueue-api always needs one.
	cfg := Config{ServiceName: "orionqueue-api", Environment: "local", HTTPAddr: ":7080", LogLevel: "info", DatabaseURL: ""}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject an empty DatabaseURL for orionqueue-api")
	}
}

func TestLoadRejectsNonPostgresDatabaseURL(t *testing.T) {
	t.Setenv("ORIONQUEUE_DATABASE_URL", "mysql://u:p@localhost/db")
	if _, err := Load("orionqueue-api"); err == nil {
		t.Fatal("expected Load to reject a non-postgres ORIONQUEUE_DATABASE_URL")
	}
}

func TestValidateDoesNotRequireDatabaseURLForServicesThatDontUseCluster(t *testing.T) {
	cfg := Config{ServiceName: "some-future-service", Environment: "local", HTTPAddr: ":7081", LogLevel: "info"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected a non-cluster service to validate without a DatabaseURL, got: %v", err)
	}
}

func TestValidateRequiresDatabaseURLForScheduler(t *testing.T) {
	cfg := Config{ServiceName: "orionqueue-scheduler", Environment: "local", HTTPAddr: ":7081", LogLevel: "info", SchedulingIntervalSeconds: 5}
	if err := cfg.Validate(); err == nil {
		t.Error("expected orionqueue-scheduler to require a non-empty DatabaseURL")
	}
}

func TestDefaultEtcdEndpointsIsSetForClusterServicesOnly(t *testing.T) {
	api := Defaults("orionqueue-api")
	if len(api.EtcdEndpoints) == 0 {
		t.Error("expected orionqueue-api to have non-empty default EtcdEndpoints")
	}
	// orionqueue-scheduler also needs etcd as of Phase 5 (leader election).
	scheduler := Defaults("orionqueue-scheduler")
	if len(scheduler.EtcdEndpoints) == 0 {
		t.Error("expected orionqueue-scheduler to have non-empty default EtcdEndpoints")
	}
	other := Defaults("some-future-service")
	if len(other.EtcdEndpoints) != 0 {
		t.Errorf("expected an unrecognized service to have empty default EtcdEndpoints, got %v", other.EtcdEndpoints)
	}
}

func TestDefaultSchedulingIntervalIsPositive(t *testing.T) {
	cfg := Defaults("orionqueue-scheduler")
	if cfg.SchedulingIntervalSeconds <= 0 {
		t.Errorf("SchedulingIntervalSeconds = %d, want > 0", cfg.SchedulingIntervalSeconds)
	}
}

func TestLoadAppliesSchedulingIntervalOverride(t *testing.T) {
	t.Setenv("ORIONQUEUE_SCHEDULING_INTERVAL_SECONDS", "10")
	cfg, err := Load("orionqueue-scheduler")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.SchedulingIntervalSeconds != 10 {
		t.Errorf("SchedulingIntervalSeconds = %d, want 10", cfg.SchedulingIntervalSeconds)
	}
}

func TestLoadRejectsNonNumericSchedulingInterval(t *testing.T) {
	t.Setenv("ORIONQUEUE_SCHEDULING_INTERVAL_SECONDS", "not-a-number")
	if _, err := Load("orionqueue-scheduler"); err == nil {
		t.Fatal("expected Load to reject a non-numeric ORIONQUEUE_SCHEDULING_INTERVAL_SECONDS")
	}
}

func TestLoadRejectsZeroSchedulingInterval(t *testing.T) {
	t.Setenv("ORIONQUEUE_SCHEDULING_INTERVAL_SECONDS", "0")
	if _, err := Load("orionqueue-scheduler"); err == nil {
		t.Fatal("expected Load to reject a zero ORIONQUEUE_SCHEDULING_INTERVAL_SECONDS")
	}
}

func TestLoadSplitsEtcdEndpointsOnComma(t *testing.T) {
	t.Setenv("ORIONQUEUE_ETCD_ENDPOINTS", "etcd-1:2379,etcd-2:2379,etcd-3:2379")
	cfg, err := Load("orionqueue-api")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	want := []string{"etcd-1:2379", "etcd-2:2379", "etcd-3:2379"}
	if len(cfg.EtcdEndpoints) != len(want) {
		t.Fatalf("EtcdEndpoints = %v, want %v", cfg.EtcdEndpoints, want)
	}
	for i, ep := range want {
		if cfg.EtcdEndpoints[i] != ep {
			t.Errorf("EtcdEndpoints[%d] = %q, want %q", i, cfg.EtcdEndpoints[i], ep)
		}
	}
}

func TestLoadRejectsEmptyEtcdEndpointsForAPI(t *testing.T) {
	cfg := Config{
		ServiceName: "orionqueue-api", Environment: "local", HTTPAddr: ":7080", LogLevel: "info",
		DatabaseURL: "postgres://u:p@localhost/db",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject empty EtcdEndpoints for orionqueue-api")
	}
}

func TestLoadIgnoresEmptyEnvValues(t *testing.T) {
	t.Setenv("ORIONQUEUE_ENVIRONMENT", "")

	cfg, err := Load("orionqueue-api")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.Environment != "local" {
		t.Errorf("Environment = %q, want default %q when env var is empty", cfg.Environment, "local")
	}
}

func TestLoadRejectsInvalidLogLevel(t *testing.T) {
	t.Setenv("ORIONQUEUE_LOG_LEVEL", "verbose")

	if _, err := Load("orionqueue-api"); err == nil {
		t.Fatal("expected Load to reject an invalid log level, got nil error")
	}
}

func TestLoadRejectsMalformedHTTPAddr(t *testing.T) {
	cases := []string{"no-colon-here", ":not-a-port", ":-1", ":99999"}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			t.Setenv("ORIONQUEUE_HTTP_ADDR", addr)
			if _, err := Load("orionqueue-api"); err == nil {
				t.Errorf("expected Load to reject HTTP addr %q, got nil error", addr)
			}
		})
	}
}

func TestValidateAcceptsAllLogLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		cfg := Config{ServiceName: "svc", Environment: "local", HTTPAddr: ":8080", LogLevel: level}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with LogLevel %q returned error: %v", level, err)
		}
	}
}

func TestDefaultPreemptionEnabledIsFalse(t *testing.T) {
	cfg := Defaults("orionqueue-scheduler")
	if cfg.PreemptionEnabled {
		t.Error("expected PreemptionEnabled to default to false (ADR-0005: off by default)")
	}
}

func TestLoadAppliesPreemptionEnabledOverride(t *testing.T) {
	t.Setenv("ORIONQUEUE_PREEMPTION_ENABLED", "true")
	cfg, err := Load("orionqueue-scheduler")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if !cfg.PreemptionEnabled {
		t.Error("expected PreemptionEnabled = true")
	}
}

func TestLoadRejectsNonBooleanPreemptionEnabled(t *testing.T) {
	t.Setenv("ORIONQUEUE_PREEMPTION_ENABLED", "not-a-bool")
	if _, err := Load("orionqueue-scheduler"); err == nil {
		t.Fatal("expected Load to reject a non-boolean ORIONQUEUE_PREEMPTION_ENABLED")
	}
}
