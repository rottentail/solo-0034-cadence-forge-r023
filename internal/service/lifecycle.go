package service

import (
	"cadenceforge/internal/domain"
	"time"
)

// Cancel and Retry are application boundaries: they hold the mutation lock and
// open one durable transaction, but every rule about which state may move where
// is decided by the domain lifecycle policy. Cancellation is acknowledged as
// soon as the decision is durable; process teardown happens after the commit so
// callers never wait on IO that is not promised to finish synchronously.
func (s *Service) Cancel(id string) (domain.Render, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	var out domain.Render
	err := s.Store.Update(func(st *domain.State) error {
		r, e := st.Cancel(id, time.Now().UTC())
		if e != nil {
			return e
		}
		out = r
		return nil
	})
	if err == nil {
		s.activeMu.Lock()
		if cancel := s.active[id]; cancel != nil {
			cancel()
		}
		s.activeMu.Unlock()
	}
	return out, err
}

func (s *Service) Retry(id string) (domain.Render, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	var out domain.Render
	err := s.Store.Update(func(st *domain.State) error {
		r, e := st.Retry(id, s.Config.QueueLimit, time.Now().UTC())
		if e != nil {
			return e
		}
		out = r
		return nil
	})
	if err == nil {
		s.Notify()
	}
	return out, err
}
