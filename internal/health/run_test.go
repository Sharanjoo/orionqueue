package health

import (
	"net"
	"net/http"
	"testing"
	"time"
)

func TestRunReturnsErrorWhenAddressAlreadyInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	blocking := &http.Server{Addr: addr}
	go blocking.Serve(ln)
	defer blocking.Close()
	time.Sleep(50 * time.Millisecond) // let the blocking server start accepting

	srv := &http.Server{Addr: addr}
	if err := Run(discardLogger(), srv, time.Second); err == nil {
		t.Fatal("expected Run to return an error when the address is already in use")
	}
}

func TestRunReturnsNilWhenServerClosedExternally(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil { // release the port for Run's ListenAndServe
		t.Fatalf("failed to release reserved port: %v", err)
	}

	srv := &http.Server{Addr: addr}
	done := make(chan error, 1)
	go func() { done <- Run(discardLogger(), srv, time.Second) }()

	time.Sleep(100 * time.Millisecond) // let ListenAndServe bind before closing
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil after external Close", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the server was closed")
	}
}
