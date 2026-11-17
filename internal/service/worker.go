package service

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/encoding"
	"cadenceforge/internal/inspect"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func (s *Service) Start(ctx context.Context) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	for i := 0; i < s.Config.Workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.loop(ctx) }()
	}
	s.Notify()
	return wg
}
func (s *Service) loop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		runCtx, cancel := context.WithCancel(ctx)
		r, ok, err := s.claim(cancel)
		if err != nil {
			slog.Error("claim render", "error", err)
		}
		if ok {
			s.execute(runCtx, r)
			cancel()
			s.activeMu.Lock()
			delete(s.active, r.ID)
			s.activeMu.Unlock()
			continue
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}
	}
}

// claim asks the domain policy to start the oldest queued render's next
// attempt inside one durable transaction, then registers the OS-level cancel
// handle only after that decision is durable.
func (s *Service) claim(cancel context.CancelFunc) (domain.Render, bool, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	var out domain.Render
	found := false
	err := s.Store.Update(func(st *domain.State) error {
		id := oldestQueued(st)
		if id == "" {
			return nil
		}
		r, ok, e := st.StartAttempt(id, time.Now().UTC())
		if e != nil {
			return e
		}
		out = r
		found = ok
		return nil
	})
	if err == nil && found {
		s.activeMu.Lock()
		s.active[out.ID] = cancel
		s.activeMu.Unlock()
	}
	return out, found && err == nil, err
}

func oldestQueued(st *domain.State) string {
	ids := []string{}
	for id, r := range st.Renders {
		if r.State == domain.StateQueued {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := st.Renders[ids[i]], st.Renders[ids[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return ids[i] < ids[j]
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

// execute runs the physical work outside the mutation lock. The lifecycle
// decision is not taken here: when the IO finishes, execute hands the observed
// effect to the policy at the commit boundary. Two distinct boundaries matter:
//
//   - Physical effects (encoding, inspection, promotion) happen first and can
//     be abandoned; an orphaned directory is retained conservatively, never
//     treated as proof of success.
//   - The durable terminal decision happens in one transaction, and only while
//     the render is still this exact running attempt. A cancellation or a
//     restart interruption that landed first makes the commit a no-op, so a
//     late old attempt can never publish over a newer fact.
func (s *Service) execute(ctx context.Context, r domain.Render) {
	attempt := len(r.Attempts)
	stage, err := s.Blobs.Staging(r.ID, attempt)
	if err != nil {
		s.complete(r.ID, attempt, nil, err)
		return
	}
	defer os.RemoveAll(stage)
	for _, source := range r.Sources {
		path, e := s.Blobs.Object(source.Digest)
		if e != nil {
			s.complete(r.ID, attempt, nil, e)
			return
		}
		digest, n, e := inspect.Digest(path)
		if e != nil {
			s.complete(r.ID, attempt, nil, e)
			return
		}
		if digest != source.Digest || n != source.Bytes {
			s.complete(r.ID, attempt, nil, fmt.Errorf("source content integrity failed"))
			return
		}
	}
	runner := encoding.Runner{Binary: s.Config.FFmpeg, Blobs: s.Blobs}
	name, err := runner.Encode(ctx, r, stage)
	if err != nil {
		s.complete(r.ID, attempt, nil, err)
		return
	}
	inspection, err := inspect.Artifact(ctx, s.Probe, filepath.Join(stage, name), r)
	if err != nil {
		s.complete(r.ID, attempt, nil, err)
		return
	}
	artifact := &domain.Artifact{Filename: name, CaptionFile: "captions.vtt", Inspection: inspection}

	s.mutation.Lock()
	defer s.mutation.Unlock()
	st, err := s.Store.Snapshot()
	if err != nil {
		slog.Error("read before promotion", "error", err)
		return
	}
	current, ok := st.Renders[r.ID]
	// Commit boundary precondition: the render is still this attempt and still
	// running. If cancel or restart already decided it, the finished work is
	// discarded with the staging directory and no state is written.
	if !ok || current.State != domain.StateRunning || len(current.Attempts) != attempt {
		return
	}
	if err = s.Blobs.Promote(stage, r.ID, attempt); err != nil {
		s.completeLocked(r.ID, attempt, nil, err)
		return
	}
	s.completeLocked(r.ID, attempt, artifact, nil)
}

func (s *Service) complete(id string, attempt int, a *domain.Artifact, cause error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	s.completeLocked(id, attempt, a, cause)
}

// completeLocked opens the single durable transaction that turns an attempt's
// observed effect into a lifecycle fact. The policy rejects any render that is
// no longer the same running attempt, so mixed facts are impossible even under
// real process-exit delays or forced termination.
func (s *Service) completeLocked(id string, attempt int, a *domain.Artifact, cause error) {
	err := s.Store.Update(func(st *domain.State) error {
		_, e := st.CompleteAttempt(id, attempt, a, cause, time.Now().UTC())
		return e
	})
	if err != nil {
		slog.Error("commit render result", "render", id, "error", err)
	}
}
