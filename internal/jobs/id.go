package jobs

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// newRandomID generates an unpredictable ID with the given prefix (e.g.
// "job"), using crypto/rand directly rather than adding a UUID dependency
// — this package has no external dependencies and there's no need for one
// just for ID generation.
func newRandomID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand.Read on a supported platform only fails if the OS
		// entropy source itself is broken, which is unrecoverable; panic
		// rather than silently hand out a low-entropy or empty ID.
		panic(fmt.Sprintf("jobs: crypto/rand unavailable: %v", err))
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}
