package main

import (
	"context"
	"fmt"

	"agentflow/flow"
)

// State is the single typed value shared by every node in the workflow.
// Because it's generic (Workflow[T]), every node function receives *State,
// so they can read and write the same fields without casting.
type State struct {
	Value  int
	Visits []string
}

func main() {
	wf := flow.New[State]("hello-world")

	// Task is the simplest node: run a function, then move to Next.
	// The first node added becomes the start node.
	wf.Task("start", func(ctx context.Context, s *State) error {
		fmt.Println("  [start] beginning")
		s.Value = 1
		s.Visits = append(s.Visits, "start")
		return nil
	}).Next("double")

	wf.Task("double", func(ctx context.Context, s *State) error {
		fmt.Printf("  [double] value before: %d\n", s.Value)
		s.Value *= 2
		s.Visits = append(s.Visits, "double")
		return nil
	}).Next("finish")

	// Succeed is a terminal node. It ends the run successfully.
	wf.Succeed("finish")

	// Run validates the graph first, then executes.
	// It returns *Result[T] (final status, state, trace, step count) and an
	// error if the workflow failed.
	var state State
	result, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}

	fmt.Printf("\nFinal status: %s\n", result.Status)
	fmt.Printf("Final value:  %d\n", state.Value)
	fmt.Printf("Visits:       %v\n", state.Visits)
	fmt.Printf("Steps taken:  %d\n", result.Steps)
	fmt.Printf("Last node:    %s\n", result.LastNode)
}

/*
Expected output:

  [start] beginning
  [double] value before: 1

Final status: succeeded
Final value:  2
Visits:       [start double]
Steps taken:  3
Last node:    finish

Key takeaways:
- New[T] creates a generic, typed workflow.
- The first node added ("start") becomes the start node automatically.
- Task functions mutate shared state directly (no copying).
- Next("target") links nodes into a static graph.
- Succeed is terminal; Runs stops when it reaches a terminal node.
- Result contains the final status, step count, and last node.
*/