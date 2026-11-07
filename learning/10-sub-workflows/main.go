package main

import (
	"context"
	"errors"
	"fmt"

	"agentflow/flow"
)

// State is shared across parent and sub-workflows.
type State struct {
	Value   int
	Log     []string
	Recover bool
}

func logHelper(s *State, msg string) {
	s.Log = append(s.Log, msg)
}

func runOK(_ *State, res *flow.Result[State], err error) {
	if err != nil {
		panic(fmt.Sprintf("Run returned error: %v", err))
	}
	if res.Status != flow.StatusSucceeded {
		panic(fmt.Sprintf("status = %q, want %q", res.Status, flow.StatusSucceeded))
	}
}

func main() {
	fmt.Println("=== Example A: Basic sub-workflow ===")
	runBasic()

	fmt.Println("\n=== Example B: Sub-workflow error handling ===")
	runErrorHandling()

	fmt.Println("\n=== Example C: Nested sub-workflows ===")
	runNested()
}

func runBasic() {
	// Build a reusable sub-workflow that processes a document.
	sub := flow.New[State]("process-doc")
	sub.Task("load", func(_ context.Context, s *State) error {
		logHelper(s, "sub: load document")
		s.Value++
		return nil
	}).Next("validate")
	sub.Task("validate", func(_ context.Context, s *State) error {
		logHelper(s, "sub: validate document")
		s.Value++
		return nil
	}).Next("done")
	sub.Succeed("done")

	// Parent workflow uses the sub-workflow as a single node.
	wf := flow.New[State]("parent")
	wf.Task("start", func(_ context.Context, s *State) error {
		logHelper(s, "parent: start")
		s.Value = 0
		return nil
	}).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("report")
	wf.Task("report", func(_ context.Context, s *State) error {
		logHelper(s, "parent: report")
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state State
	res, err := wf.Run(context.Background(), &state)
	runOK(&state, res, err)

	fmt.Printf("Status:   %s\n", res.Status)
	fmt.Printf("Value:    %d (want 2 - incremented twice in sub-workflow)\n", state.Value)
	fmt.Printf("Steps:    %d (start + sub-workflow + report + done)\n", res.Steps)
	fmt.Printf("Log:\n")
	for _, entry := range state.Log {
		fmt.Printf("  - %s\n", entry)
	}
}

func runErrorHandling() {
	boom := errors.New("sub-workflow failed")

	// Sub-workflow that fails at runtime.
	sub := flow.New[State]("failing-sub")
	sub.Task("bad", func(context.Context, *State) error {
		return boom
	}).Next("done")
	sub.Succeed("done")

	// Parent without Catch: error propagates with SubWorkflowError + trace.
	wf := flow.New[State]("parent-no-catch")
	wf.Task("setup", func(context.Context, *State) error { return nil }).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("report")
	wf.Task("report", func(context.Context, *State) error { return nil }).Next("done")
	wf.Succeed("done")

	var state State
	_, err := wf.Run(context.Background(), &state)
	if err == nil {
		fmt.Println("ERROR: expected sub-workflow failure")
		return
	}

	if swe, ok := errors.AsType[*flow.SubWorkflowError](err); ok {
		fmt.Printf("Caught *flow.SubWorkflowError:\n")
		fmt.Printf("  Err:   %v\n", swe.Err)
		fmt.Printf("  Trace: %d events from inside sub-workflow\n", len(swe.Trace))
		for _, ev := range swe.Trace {
			fmt.Printf("    %s node=%s\n", ev.Kind, ev.Node)
		}
		if errors.Is(err, boom) {
			fmt.Printf("  errors.Is(boom): true\n")
		}
	} else {
		fmt.Printf("Got unexpected error type: %T: %v\n", err, err)
	}

	// Parent with Catch: error route handles the sub-workflow failure.
	wf2 := flow.New[State]("parent-with-catch")
	wf2.Task("setup", func(context.Context, *State) error { return nil }).Next("run-sub")
	wf2.SubWorkflow("run-sub", sub).
		Catch("recover", flow.MatchAs(&flow.SubWorkflowError{})).
		Next("report")
	wf2.Task("recover", func(_ context.Context, s *State) error {
		s.Recover = true
		return nil
	}).Next("report")
	wf2.Task("report", func(context.Context, *State) error { return nil }).Next("done")
	wf2.Succeed("done")

	var state2 State
	res2, err2 := wf2.Run(context.Background(), &state2)
	runOK(&state2, res2, err2)
	if !state2.Recover {
		fmt.Println("ERROR: recovery task did not run")
		return
	}
	fmt.Println("Sub-workflow error was caught and recovered via Catch route")
}

func runNested() {
	var state State

	// Innermost sub-workflow.
	inner := flow.New[State]("inner")
	inner.Task("inner-task", func(_ context.Context, s *State) error {
		logHelper(s, "inner: processing")
		s.Value++
		return nil
	}).Next("inner-done")
	inner.Succeed("inner-done")

	// Middle sub-workflow contains the inner one.
	middle := flow.New[State]("middle")
	middle.Task("mid-prep", func(context.Context, *State) error {
		logHelper(&state, "middle: preparing")
		return nil
	}).Next("run-inner")
	middle.SubWorkflow("run-inner", inner).Next("mid-done")
	middle.Succeed("mid-done")

	// Parent contains the middle sub-workflow.
	wf := flow.New[State]("parent")
	wf.Task("start", func(_ context.Context, s *State) error {
		logHelper(s, "parent: starting")
		s.Value = 0
		return nil
	}).Next("run-middle")
	wf.SubWorkflow("run-middle", middle).Next("done")
	wf.Succeed("done")

	res, err := wf.Run(context.Background(), &state)
	runOK(&state, res, err)

	fmt.Printf("Status:   %s\n", res.Status)
	fmt.Printf("Value:    %d (want 1 - innermost task incremented)\n", state.Value)
	fmt.Printf("Steps:    %d (start + sub-workflow + done)\n", res.Steps)
	fmt.Printf("Log:\n")
	for _, entry := range state.Log {
		fmt.Printf("  - %s\n", entry)
	}
}

/*
Key takeaways:

- Sub-workflows run synchronously as a single node in the parent graph.
  They share the parent's *State, so data flows in and out naturally.
- A sub-workflow counts as one opaque step against the parent's MaxSteps
  budget. Its internal nodes count against its own internal budget.
- On failure, a *flow.SubWorkflowError is returned, wrapping the terminal
  error and attaching the sub-workflow's full execution trace. Use
  flow.MatchAs or flow.MatchIs on Catch routes to handle it.
- Sub-workflows can nest to any depth: a sub-workflow can itself contain
  sub-workflow nodes.
- Mermaid renders sub-workflow nodes with a subroutine-like [[...]] shape
  and a green subworkflow class.
*/
