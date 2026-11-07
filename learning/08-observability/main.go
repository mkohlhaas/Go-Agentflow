package main

import (
	"context"
	"fmt"

	"agentflow/flow"
)

type State struct {
	Value int
	Log   []string
}

func main() {
	wf := flow.New[State]("observability-demo")

	wf.Task("load", func(_ context.Context, s *State) error {
		s.Value = 42
		s.Log = append(s.Log, "loaded")
		return nil
	}).Next("check")

	wf.Choice("check", func(_ context.Context, s *State) (string, error) {
		if s.Value > 0 {
			return "ok", nil
		}
		return "", nil
	}).When("ok", "work").Otherwise("fail")

	wf.Task("work", func(_ context.Context, s *State) error {
		s.Value *= 2
		s.Log = append(s.Log, "worked")
		return nil
	}).Next("done")

	wf.Succeed("done")
	wf.Fail("fail", "value was not positive")

	// --- Observer ---------------------------------------------------------
	// WithObserver receives every trace event as it happens. It is called
	// from the goroutine driving the run, never from parallel branches, so
	// it does not need to be concurrency-safe.
	var observed []flow.Event
	var state State
	result, err := wf.Run(context.Background(), &state,
		flow.WithObserver(func(ev flow.Event) {
			observed = append(observed, ev)
			fmt.Printf("  [observer] %-25s node=%s seq=%d\n",
				ev.Kind, ev.Node, ev.Seq)
		}),
	)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}

	fmt.Printf("\nObserver saw %d events total\n", len(observed))

	// --- Trace ------------------------------------------------------------
	// result.Trace holds the same events in order. Each event has a
	// monotonically increasing Seq number, making traces easy to compare
	// and diff in tests.
	fmt.Printf("\nTrace events:\n")
	for i, ev := range result.Trace {
		extra := ""
		if ev.Route != "" {
			extra += fmt.Sprintf(" route=%s", ev.Route)
		}
		if ev.Tool != "" {
			extra += fmt.Sprintf(" tool=%s", ev.Tool)
		}
		if ev.Message != "" {
			extra += fmt.Sprintf(" msg=%s", ev.Message)
		}
		fmt.Printf("  %2d: %-20s node=%-10s%s\n", i+1, ev.Kind, ev.Node, extra)
	}

	// --- Mermaid ----------------------------------------------------------
	// Mermaid() returns a deterministic flowchart (declaration order is
	// preserved, so repeated calls produce identical output).
	fmt.Printf("\nMermaid diagram:\n%s\n", wf.Mermaid())
}

/*
Key takeaways:

- Result.Trace is a []Event with Seq, Time, Kind, Node, Branch, Step,
  Attempt, Route, Tool, and Message fields. It is deliberately flat so
  traces are easy to serialize, compare in tests, and print.
- WithObserver(fn) delivers every event as it happens. The observer runs
  on the driver goroutine — not from parallel branches — so it doesn't
  need locking.
- Event kinds include node_started, node_finished, node_failed, retry,
  branch_started/finished/failed, route_selected, decision_rejected,
  tool_loop_thought, tool_started/finished/failed/rejected, tool_loop_finished,
  workflow_finished/failed/canceled.
- Mermaid() renders the static graph as a flowchart TD diagram. Node
  declaration order is preserved, so output is deterministic across calls.
  Error routes are dotted edges labeled "error"; Parallel branches are
  drawn in subgraphs with fan-out and join edges.
- Use traces for debugging and audit logs; use Mermaid for documentation
  and visualizing the static graph structure.
*/
