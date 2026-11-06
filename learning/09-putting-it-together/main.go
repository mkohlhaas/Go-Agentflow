package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"agentflow/flow"
)

// State is the typed state shared across every node.
type State struct {
	Counter int64
	Log     []string
}

// logHelper appends to the state log (called from single-threaded nodes).
func logHelper(s *State, msg string) {
	s.Log = append(s.Log, msg)
}

// formatPolicy is the Decision policy for the "decide-format" node.
// It simulates a mock LLM that picks a declared route option.
type formatPolicy struct{}

func (formatPolicy) Choose(_ context.Context, s *State, opts []flow.RouteOption) (string, error) {
	logHelper(s, "decide-format: policy evaluating options")
	// Simulate an LLM choosing "markdown"
	return "markdown", nil
}

// stateCopy returns a shallow copy of State for the isolated branch copy.
func stateCopy(s *State) *State {
	cp := *s
	cp.Log = append([]string(nil), s.Log...)
	return &cp
}

func main() {
	fmt.Println("=== Building a document-processing workflow ===")

	wf := flow.New[State]("document-processor")

	// 1. Task: ingest the document
	wf.Task("ingest", func(_ context.Context, s *State) error {
		logHelper(s, "ingest: document received")
		atomic.AddInt64(&s.Counter, 1)
		return nil
	}).Next("decide-format")

	// 2. Decision: an LLM (mocked) chooses the format path.
	// The policy only sees declared options — it cannot invent new routes.
	wf.Decision("decide-format", flow.DecisionSpec[State]{
		Options: []flow.RouteOption{
			{Key: "markdown", Description: "Convert to Markdown", Target: "convert-md"},
			{Key: "pdf", Description: "Convert to PDF", Target: "convert-pdf"},
			{Key: "skip", Description: "Skip conversion", Target: "finalize"},
		},
		Choose:      formatPolicy{},
		MaxAttempts: 2,
	})

	// 3. Task: convert to Markdown
	wf.Task("convert-md", func(_ context.Context, s *State) error {
		logHelper(s, "convert-md: rendering markdown")
		atomic.AddInt64(&s.Counter, 1)
		return nil
	}).Next("review")

	// 4. Task: convert to PDF (alternate path, not taken in this run)
	wf.Task("convert-pdf", func(_ context.Context, s *State) error {
		logHelper(s, "convert-pdf: rendering pdf")
		atomic.AddInt64(&s.Counter, 1)
		return nil
	}).Next("review")

	// 5. Choice: deterministic gate based on state
	wf.Choice("review", func(_ context.Context, s *State) (string, error) {
		logHelper(s, "review: checking counter")
		if atomic.LoadInt64(&s.Counter) >= 2 {
			return "approved", nil
		}
		return "rejected", nil
	}).When("approved", "finalize").When("rejected", "fail")

	// 6. ParallelIsolated: run two independent checks on isolated copies,
	// then merge results back — no mutex needed.
	wf.ParallelIsolated("finalize",
		func(s *State) *State {
			return stateCopy(s)
		},
		func(main *State, copies []*State) error {
			logHelper(main, "finalize: merging parallel check results")
			for _, c := range copies {
				main.Log = append(main.Log, c.Log...)
			}
			return nil
		},
		flow.Branch[State]{
			Name: "spellcheck",
			Run: func(_ context.Context, s *State) error {
				logHelper(s, "spellcheck: passed")
				return nil
			},
		},
		flow.Branch[State]{
			Name: "lint",
			Run: func(_ context.Context, s *State) error {
				logHelper(s, "lint: passed")
				return nil
			},
		},
	).Next("publish")

	// 7. Task: publish
	wf.Task("publish", func(_ context.Context, s *State) error {
		logHelper(s, "publish: document published")
		return nil
	}).Next("done")

	// 8. Terminal nodes
	wf.Succeed("done")
	wf.Fail("fail", "review rejected the document")

	var state State
	result, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("Workflow failed: %v\n", err)
		return
	}

	fmt.Printf("\n=== Workflow complete ===\n")
	fmt.Printf("Status:   %s\n", result.Status)
	fmt.Printf("Steps:    %d\n", result.Steps)
	fmt.Printf("Counter:  %d\n", state.Counter)
	fmt.Printf("Log:\n")
	for _, entry := range state.Log {
		fmt.Printf("  - %s\n", entry)
	}

	// Show Mermaid diagram of the full graph.
	fmt.Printf("\n--- Workflow graph ---\n%s\n", wf.Mermaid())
}

/*
Key takeaways:

This example combines:
- Task       : deterministic work units
- Decision   : LLM-driven route choice with a whitelist of options
- Choice     : deterministic branch on computed state
- ParallelIsolated: concurrent work on isolated state copies, merged after
- Succeed/Fail: terminal nodes

The graph is static and can be exported as Mermaid. Every LLM decision is
validated before execution. Loops and agent iterations are bounded.
Context cancellation is respected throughout.

You can now extend this pattern to your own workflows by adding more node
types, retry policies, error routes, and observer hooks as needed.
*/