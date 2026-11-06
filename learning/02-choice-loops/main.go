package main

import (
	"context"
	"fmt"

	"agentflow/flow"
)

// State tracks how many times we've incremented and the path taken.
type State struct {
	Count int
	Path  []string
}

func main() {
	wf := flow.New[State]("choice-loop")

	// inc is a Task that increments the counter.
	wf.Task("inc", func(ctx context.Context, s *State) error {
		s.Count++
		s.Path = append(s.Path, "inc")
		fmt.Printf("  [inc] count = %d\n", s.Count)
		return nil
	}).Next("check")

	// Choice is a deterministic branch: the function returns a route key,
	// and the engine follows the matching When/Otherwise declaration.
	//
	// Returning "" selects the Otherwise route (the default fallback).
	wf.Choice("check", func(ctx context.Context, s *State) (string, error) {
		if s.Count < 3 {
			return "again", nil // loop back to "inc"
		}
		return "", nil // empty key => Otherwise => "done"
	}).When("again", "inc").Otherwise("done")

	// Succeed terminates the workflow.
	wf.Succeed("done")

	var state State
	result, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}

	fmt.Printf("\nFinal status: %s\n", result.Status)
	fmt.Printf("Final count:  %d\n", state.Count)
	fmt.Printf("Path:         %v\n", state.Path)
	fmt.Printf("Steps:        %d\n", result.Steps)

	// Expected:
	//   inc -> check -> inc -> check -> inc -> check -> done
	//   count = 3, steps = 6 (3 tasks + 3 choices)
}

/*
Key takeaways:

- ChoiceFunc returns a string route key. The engine looks it up in the
  node's Routes map (built from When/Otherwise calls).
- An empty key selects Otherwise. Unknown keys fail the node.
- Graph cycles are allowed (inc -> check -> inc), but they are bounded by
  DefaultMaxSteps (1000 node visits). A runaway loop will fail with
  flow.ErrMaxSteps instead of looping forever.
- Choice is deterministic — no LLM involved. Use Choice for conditions you
  can compute in Go; use Decision for LLM-driven choices (see 03-decisions).
*/