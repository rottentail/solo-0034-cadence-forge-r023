package domain

// Render states are the five durable lifecycle facts. No additional states are
// introduced: the same five values are stored in state.json and emitted in the
// same events as before.
const (
	StateQueued    = "queued"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

// Attempt outcomes record what a single attempt did. "running" marks an
// in-flight attempt whose outcome is not yet a fact; the other values are the
// terminal attempt outcomes, one per execution.
const (
	OutcomeRunning     = "running"
	OutcomeSucceeded   = "succeeded"
	OutcomeFailed      = "failed"
	OutcomeCancelled   = "cancelled"
	OutcomeInterrupted = "interrupted"
)

// MaxAttempts is the independent retry/attempt rule. It is owned here so the
// policy can be reviewed without reading the worker wiring.
const MaxAttempts = 3

// DetailLimit keeps a failure detail bounded inside one durable state write.
const DetailLimit = 8192

// Terminal reports whether a render state is a frozen end state. Terminal
// renders never leave the state again through any lifecycle rule; a new task is
// the only way to re-run work.
func Terminal(state string) bool {
	return state == StateSucceeded || state == StateFailed || state == StateCancelled
}
