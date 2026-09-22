//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sharanjoo/orionqueue/internal/leases"
)

func TestRunElection_SingleInstanceBecomesLeader(t *testing.T) {
	client := newTestEtcdClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	becameLeader := make(chan struct{})
	go func() {
		_ = leases.RunElection(ctx, client, "/test/election/single", "candidate-1", 5*time.Second,
			func(leaderCtx context.Context) {
				close(becameLeader)
				<-leaderCtx.Done()
			},
			nil,
		)
	}()

	select {
	case <-becameLeader:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the single candidate to become leader")
	}
}

// TestRunElection_OnlyOneOfTwoInstancesLeadsAtATime is the most direct
// possible proof that etcd-backed leader election actually enforces
// mutual exclusion between scheduler replicas, against real etcd, not a
// fake: two independent RunElection loops campaign concurrently on the
// same key, and a shared counter (incremented/decremented exactly around
// each instance's onLeader callback) must never exceed 1.
func TestRunElection_OnlyOneOfTwoInstancesLeadsAtATime(t *testing.T) {
	client := newTestEtcdClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var activeLeaders int32
	var maxObserved int32
	observe := func(leaderCtx context.Context) {
		n := atomic.AddInt32(&activeLeaders, 1)
		for {
			cur := atomic.LoadInt32(&maxObserved)
			if n <= cur || atomic.CompareAndSwapInt32(&maxObserved, cur, n) {
				break
			}
		}
		<-leaderCtx.Done()
		atomic.AddInt32(&activeLeaders, -1)
	}

	for _, id := range []string{"candidate-a", "candidate-b"} {
		id := id
		go func() {
			_ = leases.RunElection(ctx, client, "/test/election/mutex", id, 5*time.Second, observe, nil)
		}()
	}

	// Let both candidates campaign for a while — long enough that if
	// mutual exclusion were broken, both would have been leader
	// simultaneously at some point.
	deadline := time.After(10 * time.Second)
	for atomic.LoadInt32(&maxObserved) == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for either candidate to become leader")
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(3 * time.Second) // give the second candidate a chance to (incorrectly) also lead, if it were going to

	if got := atomic.LoadInt32(&maxObserved); got > 1 {
		t.Fatalf("observed %d simultaneous leaders, want at most 1", got)
	}
}

// TestRunElection_FailoverToWaitingCandidate proves the other half of
// leader election's value: when the current leader steps down, a
// waiting candidate takes over automatically, without any external
// coordination.
func TestRunElection_FailoverToWaitingCandidate(t *testing.T) {
	client := newTestEtcdClient(t)

	leaderACtx, cancelA := context.WithCancel(context.Background())
	bCtx, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	aBecameLeader := make(chan struct{})
	aStepped := make(chan struct{})
	go func() {
		_ = leases.RunElection(leaderACtx, client, "/test/election/failover", "candidate-a", 5*time.Second,
			func(leaderCtx context.Context) {
				close(aBecameLeader)
				<-leaderCtx.Done()
				close(aStepped)
			},
			nil,
		)
	}()

	select {
	case <-aBecameLeader:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for candidate-a to become leader")
	}

	bBecameLeader := make(chan struct{})
	go func() {
		_ = leases.RunElection(bCtx, client, "/test/election/failover", "candidate-b", 5*time.Second,
			func(leaderCtx context.Context) {
				select {
				case <-bBecameLeader:
				default:
					close(bBecameLeader)
				}
				<-leaderCtx.Done()
			},
			nil,
		)
	}()

	// candidate-b must NOT become leader while candidate-a still holds it.
	select {
	case <-bBecameLeader:
		t.Fatal("candidate-b became leader while candidate-a still held leadership")
	case <-time.After(2 * time.Second):
	}

	cancelA()
	select {
	case <-aStepped:
	case <-time.After(10 * time.Second):
		t.Fatal("candidate-a did not step down after its context was canceled")
	}

	select {
	case <-bBecameLeader:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for candidate-b to take over after candidate-a stepped down")
	}
}
