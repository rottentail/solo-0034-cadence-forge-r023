package domain

import (
	"strconv"
	"time"
)

// This file is the single owner of render lifecycle rules. Every durable
// transition of a render (its State, its Attempts and its Artifact) is decided
// here. The application layer schedules IO and chooses *when* to ask for a
// decision, but it never writes State/Outcome itself, so the terminal-state
// rule and the three-attempt rule can be reviewed in one place.
//
// The rules deliberately keep the five existing states and existing events;
// they only centralize what was previously spread across service and journal.

// PendingRenders counts renders still consuming a queue slot. Capacity is an
// application concern; the counting rule that feeds it lives with the policy.
func PendingRenders(st *State) int {
	pending := 0
	for _, r := range st.Renders {
		if r.State == StateQueued || r.State == StateRunning {
			pending++
		}
	}
	return pending
}

// Cancel decides an explicit cancellation. It applies only while the render is
// undecided: a render already in a terminal state other than cancelled cannot
// be cancelled. Cancelling an already-cancelled render is the documented
// idempotent acknowledgement and changes nothing.
//
// The decision is committed immediately, without waiting for the operating
// process to exit: the in-flight attempt is marked cancelled and its context is
// torn down by the application afterwards. A still-running old attempt cannot
// publish over this decision because CompleteAttempt only applies to a render
// that is still StateRunning with the same attempt count.
func (s *State) Cancel(id string, now time.Time) (Render, error) {
	r, ok := s.Renders[id]
	if !ok {
		return Render{}, Missing("render", id)
	}
	if r.State == StateCancelled {
		return r, nil
	}
	if Terminal(r.State) {
		return Render{}, Conflict("completed render cannot be cancelled")
	}
	r.State = StateCancelled
	r.UpdatedAt = now
	if len(r.Attempts) > 0 {
		a := &r.Attempts[len(r.Attempts)-1]
		a.Outcome = OutcomeCancelled
		a.FinishedAt = &now
	}
	s.Renders[id] = r
	s.Emit(id, "render.cancelled", "artifact visibility revoked before acknowledgement")
	return r, nil
}

// Retry moves a failed render back to the queue. It is the only rule that
// leaves a terminal state, and only the failed state may leave: succeeded and
// cancelled renders keep their frozen meaning. The three-attempt ceiling is
// enforced here independently of any single attempt's outcome.
func (s *State) Retry(id string, queueLimit int, now time.Time) (Render, error) {
	r, ok := s.Renders[id]
	if !ok {
		return Render{}, Missing("render", id)
	}
	if r.State != StateFailed {
		return Render{}, Conflict("only failed render can retry")
	}
	if len(r.Attempts) >= MaxAttempts {
		return Render{}, Conflict("three-attempt limit reached")
	}
	if PendingRenders(s) >= queueLimit {
		return Render{}, Capacity("render queue is full")
	}
	r.State = StateQueued
	r.UpdatedAt = now
	s.Renders[id] = r
	s.Emit(id, "render.retried", "original frozen inputs retained")
	return r, nil
}

// StartAttempt claims a queued render for execution and opens a new in-flight
// attempt. The attempt counter is the identity of this execution; later
// decisions are bound to it so an older attempt can never overwrite a newer
// fact.
func (s *State) StartAttempt(id string, now time.Time) (Render, bool, error) {
	r, ok := s.Renders[id]
	if !ok || r.State != StateQueued {
		return Render{}, false, nil
	}
	r.State = StateRunning
	r.UpdatedAt = now
	r.Attempts = append(r.Attempts, Attempt{Number: len(r.Attempts) + 1, StartedAt: now, Outcome: OutcomeRunning})
	s.Renders[id] = r
	s.Emit(id, "render.started", strconv.Itoa(len(r.Attempts)))
	return r, true, nil
}

// CompleteAttempt commits the effect of one execution attempt. It returns
// applied=false without changing anything when the render has already been
// decided (cancelled, interrupted by a restart, or superseded by another
// attempt): that guard is what lets a late-finishing old attempt die quietly
// instead of mixing its result into a newer fact.
//
// On success the artifact is attached in the same durable write as the
// succeeded state, so "succeeded" and "a verified artifact exists" can never
// diverge. On failure the cause is recorded on the bound attempt.
func (s *State) CompleteAttempt(id string, attempt int, artifact *Artifact, cause error, now time.Time) (bool, error) {
	r, ok := s.Renders[id]
	if !ok || r.State != StateRunning || len(r.Attempts) != attempt {
		return false, nil
	}
	r.UpdatedAt = now
	last := &r.Attempts[len(r.Attempts)-1]
	last.FinishedAt = &now
	if cause != nil {
		r.State = StateFailed
		last.Outcome = OutcomeFailed
		last.Detail = boundDetail(cause.Error())
	} else {
		r.State = StateSucceeded
		last.Outcome = OutcomeSucceeded
		r.Artifact = artifact
	}
	s.Renders[id] = r
	s.Emit(id, "render."+r.State, last.Detail)
	return true, nil
}

// Interrupt closes attempts left running when the process ended before their
// effect could be committed. It reuses the failed terminal state with a
// distinct attempt outcome rather than inventing a state, and preserves the
// frozen input snapshot untouched.
func (s *State) Interrupt(id string, now time.Time) {
	r, ok := s.Renders[id]
	if !ok || r.State != StateRunning {
		return
	}
	r.State = StateFailed
	r.UpdatedAt = now
	if len(r.Attempts) > 0 {
		a := &r.Attempts[len(r.Attempts)-1]
		a.FinishedAt = &now
		a.Outcome = OutcomeInterrupted
		a.Detail = "process ended before artifact commit"
	}
	s.Renders[id] = r
	s.Emit(id, "render.interrupted", "explicit retry required; frozen input retained")
}

func boundDetail(detail string) string {
	if len(detail) > DetailLimit {
		return detail[:DetailLimit]
	}
	return detail
}
