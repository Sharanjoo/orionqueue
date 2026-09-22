package scheduler

import "time"

// Aging parameters: a job's effective priority grows the longer it waits
// in QUEUED, so a steady stream of higher-priority submissions can't
// starve a low-priority job forever. See
// docs/adr/0002-scheduler-design.md for the rationale; these are package
// constants rather than per-deployment config for Phase 5 — a documented
// scope decision, not an oversight.
const (
	AgingInterval  = 30 * time.Second
	AgingIncrement = int32(1)
	MaxAgingBonus  = int32(20)
)

// EffectivePriority returns priority plus an aging bonus based on how
// long a job has been waiting (now - createdAt), capped at
// MaxAgingBonus. A job submitted in the future relative to now (clock
// skew, or a test fixture) gets no bonus rather than a negative one.
func EffectivePriority(priority int32, createdAt, now time.Time) int32 {
	if !now.After(createdAt) {
		return priority
	}
	waited := now.Sub(createdAt)
	bonus := int32(waited/AgingInterval) * AgingIncrement
	if bonus > MaxAgingBonus {
		bonus = MaxAgingBonus
	}
	return priority + bonus
}
