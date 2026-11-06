package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"agentflow/flow"
)

// sentinel errors for MatchIs demonstration
var (
	ErrRepoLocked = errors.New("repository is locked")
	ErrNetwork    = errors.New("network error")
)

// customErr is a custom error type for MatchAs demonstration
type customErr struct {
	msg string
}

func (e *customErr) Error() string { return e.msg }

type State struct {
	RecoveredFrom string
}

func main() {
	fmt.Println("=== Example A: WithRetry on a Task ===")
	runRetry()

	fmt.Println("\n=== Example B: Catch with MatchIs (sentinel errors) ===")
	runMatchIs()

	fmt.Println("\n=== Example C: Catch with MatchAs (custom error types) ===")
	runMatchAs()

	fmt.Println("\n=== Example D: Unhandled error fails the workflow ===")
	runUnhandled()
}

func runRetry() {
	wf := flow.New[State]("retry-demo")
	attempts := 0

	wf.Task("flaky", func(_ context.Context, s *State) error {
		attempts++
		fmt.Printf("    [flaky] attempt %d\n", attempts)
		if attempts < 3 {
			return errors.New("transient failure")
		}
		s.RecoveredFrom = "flaky-task"
		return nil
	}).WithRetry(flow.RetryPolicy{
		// MaxAttempts includes the first attempt, so 3 = first try + 2 retries.
		MaxAttempts:     3,
		InitialInterval: 10 * time.Millisecond,
		Backoff:         2, // delay doubles each retry (bounded by MaxInterval)
		MaxInterval:     100 * time.Millisecond,
	}).Next("done")

	wf.Succeed("done")

	var state State
	_, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("  workflow failed: %v\n", err)
		return
	}
	fmt.Printf("  recovered from: %s after %d attempts\n", state.RecoveredFrom, attempts)
}

func runMatchIs() {
	wf := flow.New[State]("catch-matchis")

	wf.Task("work", func(context.Context, *State) error {
		return ErrRepoLocked
	}).Catch("recover", flow.MatchIs[State](ErrRepoLocked)).Next("never")
	wf.Task("never", func(context.Context, *State) error {
		return nil
	}).Next("done")
	wf.Task("recover", func(_ context.Context, s *State) error {
		s.RecoveredFrom = "repo-locked via MatchIs"
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state State
	_, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("  workflow failed: %v\n", err)
		return
	}
	fmt.Printf("  recovered from: %s\n", state.RecoveredFrom)
}

func runMatchAs() {
	wf := flow.New[State]("catch-matchas")

	wf.Task("work", func(context.Context, *State) error {
		return &customErr{msg: "custom failure"}
	}).Catch("recover", flow.MatchAs(&customErr{})).Next("never")
	wf.Task("never", func(context.Context, *State) error {
		return nil
	}).Next("done")
	wf.Task("recover", func(_ context.Context, s *State) error {
		s.RecoveredFrom = "custom error via MatchAs"
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state State
	_, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("  workflow failed: %v\n", err)
		return
	}
	fmt.Printf("  recovered from: %s\n", state.RecoveredFrom)
}

func runUnhandled() {
	wf := flow.New[State]("unhandled")

	wf.Task("work", func(context.Context, *State) error {
		return errors.New("kaboom")
	}).Next("done")
	wf.Succeed("done")

	var state State
	_, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("  workflow failed (expected): %v\n", err)
	}
}

/*
Key takeaways:

- WithRetry attaches a retry policy to a Task. MaxAttempts includes the
  first attempt, so 1 means no retry. Backoff multiplies the delay after
  each failure (values < 1 are treated as 1). MaxInterval caps the delay.
- Retries apply to Task nodes only.
- Catch routes matching errors to another node instead of failing the
  workflow. Routes are checked in declaration order; a nil matcher catches
  any error.
- flow.MatchIs(target) returns a matcher using errors.Is — ideal for
  sentinel errors like ErrRepoLocked.
- flow.MatchAs(target) returns a matcher using errors.As — ideal for
  custom error types like *customErr.
- If no error route matches, the workflow fails with StatusFailed and the
  node's error is returned.
- Context cancellation always wins: it is not routed through Catch.
*/