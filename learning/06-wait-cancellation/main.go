package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"agentflow/flow"
)

type State struct {
	Slept bool
}

func main() {
	fmt.Println("=== Example A: Wait with context cancellation ===")
	runWithCancellation()

	fmt.Println("\n=== Example B: Wait completes normally ===")
	runNormal()
}

func runWithCancellation() {
	wf := flow.New[State]("wait-cancel")

	// Wait sleeps for a fixed duration. It is context-aware: if the context
	// is cancelled before the timer fires, Wait returns ctx.Err() immediately.
	wf.Wait("pause", time.Hour).Next("after-wait")
	wf.Task("after-wait", func(_ context.Context, s *State) error {
		s.Slept = true
		return nil
	}).Next("done")
	wf.Succeed("done")

	// Create a context that expires after 10 milliseconds. The Wait node is
	// asked to sleep for an hour, so cancellation will win.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	var state State
	start := time.Now()
	result, err := wf.Run(ctx, &state)
	elapsed := time.Since(start)

	fmt.Printf("  elapsed: %v\n", elapsed)
	fmt.Printf("  status:  %s\n", result.Status)
	fmt.Printf("  err:     %v\n", err)
	fmt.Printf("  slept:   %v (should be false — cancelled before the hour)\n", state.Slept)

	if errors.Is(err, context.DeadlineExceeded) {
		fmt.Println("  -> context cancelled as expected")
	}
}

func runNormal() {
	wf := flow.New[State]("wait-normal")

	wf.Wait("pause", 50*time.Millisecond).Next("after-wait")
	wf.Task("after-wait", func(_ context.Context, s *State) error {
		s.Slept = true
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state State
	start := time.Now()
	result, err := wf.Run(context.Background(), &state)
	elapsed := time.Since(start)

	fmt.Printf("  elapsed: %v\n", elapsed)
	fmt.Printf("  status:  %s\n", result.Status)
	fmt.Printf("  err:     %v\n", err)
	fmt.Printf("  slept:   %v (should be true — timer fired normally)\n", state.Slept)

	if err == nil && result.Status == flow.StatusSucceeded {
		fmt.Println("  -> Wait completed normally")
	}
}

/*
Key takeaways:

- Wait pauses for a fixed duration, then continues at Next.
- It is context-aware: if ctx is cancelled before the timer fires, Wait
  returns ctx.Err() and the run ends with StatusCanceled.
- Always pass a context to Run, and use exec.CommandContext (or similar)
  inside Task/tool functions so subprocesses also stop on cancellation.
- Cancellation always wins: it is not routed through Catch. A canceled run
  ends with StatusCanceled, not an error route.
- A Wait with delay <= 0 returns ctx.Err() immediately.
*/