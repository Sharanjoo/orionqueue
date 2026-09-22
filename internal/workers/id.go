package workers

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// newRandomID generates an unpredictable ID with the given prefix (e.g.
// "worker"), using crypto/rand directly rather than adding a UUID
// dependency — mirrors internal/jobs.newRandomID.
func newRandomID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("workers: crypto/rand unavailable: %v", err))
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}
