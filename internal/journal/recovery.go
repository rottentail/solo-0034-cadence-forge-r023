package journal

import (
	"cadenceforge/internal/domain"
	"time"
)

// Recover re-establishes durable facts at startup. The interruption rule itself
// lives in the domain policy; the journal only supplies the durable boundary.
func (s *Store) Recover() error {
	return s.Update(func(st *domain.State) error {
		now := time.Now().UTC()
		for id := range st.Renders {
			st.Interrupt(id, now)
		}
		return nil
	})
}
