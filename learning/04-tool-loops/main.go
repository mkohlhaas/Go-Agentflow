package main

import (
	"context"
	"fmt"

	"agentflow/flow"
)

// State is shared by every node and by every tool in the ToolLoop.
type State struct {
	Value   int
	Log     []string
	TestsOK bool
}

// addTool is a deterministic tool: it increments Value and reports back.
var addTool = flow.Tool[State]{
	Name:        "add",
	Description: "Increment the value by 1",
	Run: func(_ context.Context, s *State, _ map[string]string) (string, error) {
		s.Value++
		s.Log = append(s.Log, "add called")
		return "incremented", nil
	},
}

// verifyTool sets TestsOK based on the current value.
var verifyTool = flow.Tool[State]{
	Name:        "verify",
	Description: "Check whether the value is high enough",
	Run: func(_ context.Context, s *State, _ map[string]string) (string, error) {
		if s.Value >= 3 {
			s.TestsOK = true
			return "tests pass", nil
		}
		return "tests fail", nil
	},
}

// repairPolicy is a mock LLM that drives the ToolLoop. It receives the
// full history of previous turns (thoughts, tools, observations, errors)
// and decides what to do next.
type repairPolicy struct{}

func (repairPolicy) Next(_ context.Context, s *State, history []flow.ToolLoopTurn) (flow.ToolLoopDecision, error) {
	switch len(history) {
	case 0:
		// First turn: call "add"
		return flow.ToolLoopDecision{
			Thought: "I need to raise the value. Call add.",
			Tool:    "add",
		}, nil
	case 1:
		// Second turn: call "add" again
		return flow.ToolLoopDecision{
			Thought: "Still not high enough. Call add again.",
			Tool:    "add",
		}, nil
	case 2:
		// Third turn: call "add" a third time
		return flow.ToolLoopDecision{
			Thought: "One more add should do it.",
			Tool:    "add",
		}, nil
	case 3:
		// Fourth turn: verify the result
		return flow.ToolLoopDecision{
			Thought: "Now let me verify with the test tool.",
			Tool:    "verify",
		}, nil
	default:
		// If tests pass, we're done. Otherwise fail.
		if s.TestsOK {
			return flow.ToolLoopDecision{Done: true, Message: "goal reached"}, nil
		}
		return flow.ToolLoopDecision{Fail: true, Message: "tests still failing"}, nil
	}
}

func main() {
	wf := flow.New[State]("tool-loop-demo")

	wf.ToolLoop("repair", flow.ToolLoopSpec[State]{
		// Goal is descriptive only — it appears in traces and can be used
		// in prompts, but the engine does not interpret it.
		Goal:   "Raise Value to at least 3, then verify tests pass.",
		Policy: repairPolicy{},
		Tools: []flow.Tool[State]{
			addTool,
			verifyTool,
		},
		// MaxIterations bounds the policy/tool loop. Zero means
		// DefaultMaxToolLoopIterations (8). Here we set 10 explicitly.
		MaxIterations: 10,
	}).Next("done")

	wf.Succeed("done")

	var state State
	result, err := wf.Run(context.Background(), &state)
	if err != nil {
		fmt.Printf("workflow failed: %v\n", err)
		return
	}

	fmt.Printf("\nFinal status: %s\n", result.Status)
	fmt.Printf("Final value:  %d\n", state.Value)
	fmt.Printf("Tests OK:     %v\n", state.TestsOK)
	fmt.Printf("Log:          %v\n", state.Log)

	// Show tool-loop events from the trace.
	for _, ev := range result.Trace {
		switch ev.Kind {
		case flow.EventToolLoopThought:
			fmt.Printf("  [thought] %s\n", ev.Message)
		case flow.EventToolStarted:
			fmt.Printf("  [tool] %s started\n", ev.Tool)
		case flow.EventToolFinished:
			fmt.Printf("  [tool] %s finished: %s\n", ev.Tool, ev.Message)
		case flow.EventToolLoopFinished:
			fmt.Printf("  [tool-loop] finished: %s\n", ev.Message)
		}
	}
}

/*
Key takeaways:

- A ToolLoop node repeatedly calls a policy until it returns Done: true,
  Fail: true, or MaxIterations is reached.
- ToolLoopPolicy.Next receives ctx, state, and the full []ToolLoopTurn history.
  This is the transcript the "LLM" uses to decide its next action.
- ToolLoopDecision modes:
  * Done: true  -> ends the loop successfully
  * Fail: true  -> ends the loop with an error
  * neither     -> must name a declared Tool; Args are passed to the tool
- If the policy returns an unknown tool or no tool (without Done/Fail),
  the engine records flow.EventToolRejected and feeds it back as an
  observation in the next history turn. The policy can self-correct.
- Tool errors are also observations — they don't crash the run. The policy
  decides whether to retry or fail based on history.
- MaxIterations protects against runaway tool loops.
*/
