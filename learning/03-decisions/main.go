package main

import (
	"context"
	"fmt"

	"agentflow/flow"
)

// State carries a flag that the Decision policy can read.
type State struct {
	Approved bool
	Attempts int
}

// triagePolicy is a stand-in for an LLM. It sees only the declared options
// and the typed state — it cannot invent new routes.
type triagePolicy struct{}

func (triagePolicy) Choose(_ context.Context, s *State, options []flow.RouteOption) (string, error) {
	s.Attempts++
	fmt.Printf("    [policy] saw %d options, attempt %d\n", len(options), s.Attempts)

	// A real LLM might return garbage. The engine validates the key before
	// following it. If the key isn't declared, the node records a
	// "decision_rejected" event and retries up to MaxAttempts times.
	if s.Attempts == 1 {
		return "not-a-real-key", nil // intentionally invalid on first try
	}
	return "approve", nil
}

func main() {
	wf := flow.New[State]("decision-demo")

	wf.Task("start", func(ctx context.Context, s *State) error {
		fmt.Println("  [start] asking the policy to decide")
		return nil
	}).Next("triage")

	// Decision delegates a route choice to a policy. The policy may only
	// return one of the declared RouteOption keys.
	//
	// MaxAttempts: if the policy returns an unknown key, the engine asks
	// again (up to MaxAttempts times) before failing the node.
	wf.Decision("triage", flow.DecisionSpec[State]{
		Options: []flow.RouteOption{
			{Key: "approve", Description: "Approve the request", Target: "process"},
			{Key: "reject", Description: "Reject the request", Target: "archive"},
		},
		Choose:      triagePolicy{},
		MaxAttempts: 3,
	})

	wf.Task("process", func(ctx context.Context, s *State) error {
		s.Approved = true
		fmt.Println("  [process] request approved and processed")
		return nil
	}).Next("done")

	wf.Task("archive", func(ctx context.Context, s *State) error {
		fmt.Println("  [archive] request archived (rejected)")
		return nil
	}).Next("done")

	wf.Succeed("done")

	var state State
	result, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}

	fmt.Printf("\nFinal status: %s\n", result.Status)
	fmt.Printf("Approved:     %v\n", state.Approved)
	fmt.Printf("Policy calls: %d\n", state.Attempts)

	// Inspect the trace for decision_rejected events.
	rejected := 0
	for _, ev := range result.Trace {
		if ev.Kind == flow.EventDecisionRejected {
			rejected++
			fmt.Printf("  [trace] decision_rejected: %s (attempt %d)\n", ev.Route, ev.Attempt)
		}
	}
	fmt.Printf("Total decision_rejected events: %d\n", rejected)
}

/*
Key takeaways:

- DecisionPolicy.Choose receives ctx, state, and the declared RouteOptions.
  It returns a key string — the engine validates it against the whitelist.
- Invalid keys trigger flow.EventDecisionRejected and the policy is asked
  again, up to MaxAttempts. If it still fails, the node fails.
- MaxAttempts includes the first attempt, so 1 means "no retry."
- The policy never sees the graph structure — only options. This keeps the
  LLM (or mock policy) sandboxed: it cannot invent new routes or nodes.
- Use Decision when an LLM (or external policy) should pick a path.
  Use Choice when the path is deterministic Go logic (see 02-choice-loops).
*/