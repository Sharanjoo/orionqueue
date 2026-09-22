package leases

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// RunElection campaigns for leadership at key using client, and calls
// onLeader (blocking) once elected, for as long as this instance holds
// leadership. If the underlying etcd session is lost (e.g. a network
// partition longer than sessionTTL), RunElection cancels onLeader's
// context, waits for it to return, and re-campaigns automatically — it
// keeps running until ctx is canceled, at which point it resigns
// leadership (if held) before returning.
//
// This is what makes "duplicate scheduler instances do not make
// conflicting assignments" hold even before jobs.Service.AssignToWorkers'
// own state-check defense (see its doc comment): only one instance's
// onLeader is ever running at a time. candidateID identifies this
// instance in etcd (visible via etcdctl — useful for knowing which
// replica currently leads). onStatusChange, if non-nil, is called on
// every phase transition ("campaigning", "leader", "session-lost", ...)
// so the caller can log it; this package doesn't import a logger itself.
func RunElection(
	ctx context.Context,
	client *clientv3.Client,
	key, candidateID string,
	sessionTTL time.Duration,
	onLeader func(ctx context.Context),
	onStatusChange func(status string),
) error {
	notify := func(status string) {
		if onStatusChange != nil {
			onStatusChange(status)
		}
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		session, err := concurrency.NewSession(client,
			concurrency.WithTTL(int(sessionTTL.Seconds())),
			concurrency.WithContext(ctx),
		)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			notify("session-failed")
			select {
			case <-time.After(2 * time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		election := concurrency.NewElection(session, key)
		notify("campaigning")
		if err := election.Campaign(ctx, candidateID); err != nil {
			session.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			notify("campaign-failed")
			continue
		}
		notify("leader")

		leaderCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			onLeader(leaderCtx)
		}()

		select {
		case <-session.Done():
			notify("session-lost")
			cancel()
			<-done
		case <-ctx.Done():
			cancel()
			<-done
			resignCtx, resignCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = election.Resign(resignCtx)
			resignCancel()
			session.Close()
			return ctx.Err()
		}
		session.Close()
	}
}
