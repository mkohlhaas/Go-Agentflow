package flow_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agentflow/flow"
)

type seqState struct {
	Value    int
	Steps    []string
	Attempts int
	Flag     bool
}

func runOK[T any](t *testing.T, wf *flow.Workflow[T], state *T, opts ...flow.RunOption) *flow.Result[T] {
	t.Helper()
	res, err := wf.Run(context.Background(), state, opts...)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Status != flow.StatusSucceeded {
		t.Fatalf("status = %q, want %q", res.Status, flow.StatusSucceeded)
	}
	return res
}

func countEvents(events []flow.Event, kind flow.EventKind) int {
	n := 0
	for _, ev := range events {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

func countNodeEvents(events []flow.Event, kind flow.EventKind, node string) int {
	n := 0
	for _, ev := range events {
		if ev.Kind == kind && ev.Node == node {
			n++
		}
	}
	return n
}

func TestSequence(t *testing.T) {
	wf := flow.New[seqState]("sequence")
	wf.Task("one", func(_ context.Context, s *seqState) error {
		s.Steps = append(s.Steps, "one")
		s.Value++
		return nil
	}).Next("two")
	wf.Task("two", func(_ context.Context, s *seqState) error {
		s.Steps = append(s.Steps, "two")
		s.Value++
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if state.Value != 2 {
		t.Fatalf("Value = %d, want 2", state.Value)
	}
	if want := []string{"one", "two"}; fmt.Sprint(state.Steps) != fmt.Sprint(want) {
		t.Fatalf("Steps = %v, want %v", state.Steps, want)
	}
	if res.Steps != 3 {
		t.Fatalf("Steps = %d, want 3", res.Steps)
	}
	if res.LastNode != "done" {
		t.Fatalf("LastNode = %q, want done", res.LastNode)
	}
	for i, ev := range res.Trace {
		if ev.Seq != i+1 {
			t.Fatalf("event %d has Seq %d", i, ev.Seq)
		}
	}
	if got := res.Trace[len(res.Trace)-1].Kind; got != flow.EventWorkflowFinished {
		t.Fatalf("last event = %q, want %q", got, flow.EventWorkflowFinished)
	}
}

func TestChoiceLoopsAndUsesDefaultRoute(t *testing.T) {
	wf := flow.New[seqState]("choice")
	wf.Task("inc", func(_ context.Context, s *seqState) error {
		s.Value++
		return nil
	}).Next("check")
	wf.Choice("check", func(_ context.Context, s *seqState) (string, error) {
		if s.Value < 2 {
			return "again", nil
		}
		return "", nil
	}).When("again", "inc").Otherwise("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if state.Value != 2 {
		t.Fatalf("Value = %d, want 2", state.Value)
	}
	if res.Steps != 5 {
		t.Fatalf("Steps = %d, want 5", res.Steps)
	}
	if countEvents(res.Trace, flow.EventRouteSelected) != 2 {
		t.Fatalf("route events = %d, want 2", countEvents(res.Trace, flow.EventRouteSelected))
	}
}

func TestChoiceRejectsUnknownRoute(t *testing.T) {
	wf := flow.New[seqState]("bad-choice")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("pick")
	wf.Choice("pick", func(context.Context, *seqState) (string, error) {
		return "invented", nil
	}).Otherwise("done")
	wf.Succeed("done")

	var state seqState
	res, err := wf.Run(context.Background(), &state)
	if err == nil {
		t.Fatal("expected an error")
	}
	if res.Status != flow.StatusFailed {
		t.Fatalf("status = %q, want failed", res.Status)
	}
	if !strings.Contains(err.Error(), "unknown route") {
		t.Fatalf("error = %v, want unknown route", err)
	}
}

func TestTaskRetriesThenSucceeds(t *testing.T) {
	wf := flow.New[seqState]("retry")
	wf.Task("flaky", func(_ context.Context, s *seqState) error {
		s.Attempts++
		if s.Attempts < 3 {
			return fmt.Errorf("attempt %d failed", s.Attempts)
		}
		return nil
	}).WithRetry(flow.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		Backoff:         2,
		MaxInterval:     5 * time.Millisecond,
	}).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if state.Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", state.Attempts)
	}
	if got := countEvents(res.Trace, flow.EventRetry); got != 2 {
		t.Fatalf("retry events = %d, want 2", got)
	}
}

func TestCatchRoutesMatchingError(t *testing.T) {
	boom := errors.New("boom")
	wf := flow.New[seqState]("catch")
	wf.Task("work", func(context.Context, *seqState) error {
		return boom
	}).Catch("recover", func(err error) bool {
		return errors.Is(err, boom)
	}).Next("never")
	wf.Task("never", func(context.Context, *seqState) error {
		t.Fatal("unhandled path should not run")
		return nil
	}).Next("done")
	wf.Task("recover", func(_ context.Context, s *seqState) error {
		s.Flag = true
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if !state.Flag {
		t.Fatal("recovery task did not run")
	}
	if got := countEvents(res.Trace, flow.EventRouteSelected); got != 1 {
		t.Fatalf("route events = %d, want 1", got)
	}
}

func TestUnhandledTaskErrorFailsWorkflow(t *testing.T) {
	wf := flow.New[seqState]("fail")
	wf.Task("work", func(context.Context, *seqState) error {
		return errors.New("kaboom")
	}).Next("done")
	wf.Succeed("done")

	var state seqState
	res, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("error = %v, want kaboom", err)
	}
	if res.Status != flow.StatusFailed {
		t.Fatalf("status = %q, want failed", res.Status)
	}
	if res.LastNode != "work" {
		t.Fatalf("LastNode = %q, want work", res.LastNode)
	}
}

func TestFailNodeEndsWorkflow(t *testing.T) {
	wf := flow.New[seqState]("explicit-fail")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("give-up")
	wf.Fail("give-up", "deliberate stop")

	var state seqState
	res, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "deliberate stop") {
		t.Fatalf("error = %v, want deliberate stop", err)
	}
	if res.Status != flow.StatusBusinessFailed || res.LastNode != "give-up" {
		t.Fatalf("result = %+v, want business-failed at give-up", res)
	}
	if got := res.Trace[len(res.Trace)-1].Kind; got != flow.EventWorkflowFailed {
		t.Fatalf("last event = %q, want workflow_failed", got)
	}
}

type parState struct {
	A int64
	B int64
}

func TestParallelRunsBranchesAndJoins(t *testing.T) {
	wf := flow.New[parState]("parallel")
	wf.Parallel("fan",
		flow.Branch[parState]{Name: "a", Run: func(_ context.Context, s *parState) error {
			atomic.StoreInt64(&s.A, 1)
			return nil
		}},
		flow.Branch[parState]{Name: "b", Run: func(_ context.Context, s *parState) error {
			atomic.StoreInt64(&s.B, 2)
			return nil
		}},
	).Next("done")
	wf.Succeed("done")

	var state parState
	res := runOK(t, wf, &state)

	if state.A != 1 || state.B != 2 {
		t.Fatalf("state = %+v, want A=1 B=2", state)
	}
	if got := countEvents(res.Trace, flow.EventBranchStarted); got != 2 {
		t.Fatalf("branch started = %d, want 2", got)
	}
	if got := countEvents(res.Trace, flow.EventBranchFinished); got != 2 {
		t.Fatalf("branch finished = %d, want 2", got)
	}
}

func TestParallelBranchErrorFailsWorkflow(t *testing.T) {
	boom := errors.New("branch failed")
	wf := flow.New[parState]("parallel-fail")
	wf.Parallel("fan",
		flow.Branch[parState]{Name: "ok", Run: func(context.Context, *parState) error {
			return nil
		}},
		flow.Branch[parState]{Name: "bad", Run: func(context.Context, *parState) error {
			return boom
		}},
	).Next("done")
	wf.Succeed("done")

	var state parState
	res, err := wf.Run(context.Background(), &state)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if res.Status != flow.StatusFailed {
		t.Fatalf("status = %q, want failed", res.Status)
	}
	if got := countEvents(res.Trace, flow.EventBranchFailed); got != 1 {
		t.Fatalf("branch failed events = %d, want 1", got)
	}
}

func TestWaitStopsOnCancellation(t *testing.T) {
	wf := flow.New[struct{}]("wait")
	wf.Wait("pause", time.Hour).Next("done")
	wf.Succeed("done")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	var state struct{}
	res, err := wf.Run(ctx, &state)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if res.Status != flow.StatusCanceled {
		t.Fatalf("status = %q, want canceled", res.Status)
	}
	if res.LastNode != "pause" {
		t.Fatalf("LastNode = %q, want pause", res.LastNode)
	}
}

type decisionState struct {
	Calls int
	Seen  []flow.RouteOption
}

func TestDecisionRetriesInvalidOption(t *testing.T) {
	policy := flow.DecisionFunc[decisionState](func(_ context.Context, s *decisionState, opts []flow.RouteOption) (string, error) {
		s.Calls++
		s.Seen = append([]flow.RouteOption(nil), opts...)
		if s.Calls == 1 {
			return "invented", nil
		}
		return "yes", nil
	})

	wf := flow.New[decisionState]("decision")
	wf.Task("start", func(context.Context, *decisionState) error { return nil }).Next("pick")
	wf.Decision("pick", flow.DecisionSpec[decisionState]{
		Options: []flow.RouteOption{
			{Key: "yes", Description: "continue", Target: "done"},
			{Key: "no", Description: "stop", Target: "stop"},
		},
		Choose:      policy,
		MaxAttempts: 2,
	})
	wf.Succeed("done")
	wf.Fail("stop", "policy chose to stop")

	var state decisionState
	res := runOK(t, wf, &state)

	if state.Calls != 2 {
		t.Fatalf("Calls = %d, want 2", state.Calls)
	}
	if len(state.Seen) != 2 || state.Seen[0].Description != "continue" {
		t.Fatalf("policy saw options %+v", state.Seen)
	}
	if got := countEvents(res.Trace, flow.EventDecisionRejected); got != 1 {
		t.Fatalf("rejected events = %d, want 1", got)
	}
}

func TestDecisionExhaustsAttempts(t *testing.T) {
	policy := flow.DecisionFunc[decisionState](func(context.Context, *decisionState, []flow.RouteOption) (string, error) {
		return "invented", nil
	})

	wf := flow.New[decisionState]("decision-fail")
	wf.Task("start", func(context.Context, *decisionState) error { return nil }).Next("pick")
	wf.Decision("pick", flow.DecisionSpec[decisionState]{
		Options:     []flow.RouteOption{{Key: "yes", Target: "done"}},
		Choose:      policy,
		MaxAttempts: 2,
	})
	wf.Succeed("done")

	var state decisionState
	res, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "unknown option") {
		t.Fatalf("error = %v, want unknown option", err)
	}
	if res.Status != flow.StatusFailed {
		t.Fatalf("status = %q, want failed", res.Status)
	}
	if got := countEvents(res.Trace, flow.EventDecisionRejected); got != 2 {
		t.Fatalf("rejected events = %d, want 2", got)
	}
}

type agentState struct {
	Value   int
	History []flow.AgentTurn
}

func addTool() flow.Tool[agentState] {
	return flow.Tool[agentState]{
		Name:        "add",
		Description: "increment the value",
		Run: func(_ context.Context, s *agentState, _ map[string]string) (string, error) {
			s.Value++
			return "incremented", nil
		},
	}
}

func TestAgentLoopCallsToolsUntilDone(t *testing.T) {
	wf := flow.New[agentState]("agent")
	wf.Agent("repair", flow.AgentSpec[agentState]{
		Goal: "increment three times",
		Policy: flow.AgentFunc[agentState](func(_ context.Context, _ *agentState, history []flow.AgentTurn) (flow.AgentDecision, error) {
			if len(history) < 3 {
				return flow.AgentDecision{Thought: "add one", Tool: "add"}, nil
			}
			return flow.AgentDecision{Done: true, Message: "target reached"}, nil
		}),
		Tools:         []flow.Tool[agentState]{addTool()},
		MaxIterations: 10,
	}).Next("done")
	wf.Succeed("done")

	var state agentState
	res := runOK(t, wf, &state)

	if state.Value != 3 {
		t.Fatalf("Value = %d, want 3", state.Value)
	}
	if got := countEvents(res.Trace, flow.EventToolFinished); got != 3 {
		t.Fatalf("tool finished events = %d, want 3", got)
	}
	if got := countEvents(res.Trace, flow.EventAgentThought); got != 3 {
		t.Fatalf("agent thought events = %d, want 3", got)
	}
	if got := countEvents(res.Trace, flow.EventAgentFinished); got != 1 {
		t.Fatalf("agent finished events = %d, want 1", got)
	}
}

func TestAgentFeedsUnknownToolBackToPolicy(t *testing.T) {
	wf := flow.New[agentState]("agent-unknown")
	wf.Agent("repair", flow.AgentSpec[agentState]{
		Policy: flow.AgentFunc[agentState](func(_ context.Context, s *agentState, history []flow.AgentTurn) (flow.AgentDecision, error) {
			switch len(history) {
			case 0:
				return flow.AgentDecision{Tool: "missing"}, nil
			case 1:
				return flow.AgentDecision{Tool: "add"}, nil
			default:
				s.History = append([]flow.AgentTurn(nil), history...)
				return flow.AgentDecision{Done: true}, nil
			}
		}),
		Tools:         []flow.Tool[agentState]{addTool()},
		MaxIterations: 5,
	}).Next("done")
	wf.Succeed("done")

	var state agentState
	res := runOK(t, wf, &state)

	if len(state.History) != 2 {
		t.Fatalf("history length = %d, want 2", len(state.History))
	}
	if !strings.Contains(state.History[0].Error, "unknown tool") {
		t.Fatalf("first history error = %q, want unknown tool", state.History[0].Error)
	}
	if got := countEvents(res.Trace, flow.EventToolRejected); got != 1 {
		t.Fatalf("tool rejected events = %d, want 1", got)
	}
}

func TestAgentToolErrorIsAnObservation(t *testing.T) {
	failing := flow.Tool[agentState]{
		Name: "fail",
		Run: func(context.Context, *agentState, map[string]string) (string, error) {
			return "partial", errors.New("tool exploded")
		},
	}
	wf := flow.New[agentState]("agent-tool-error")
	wf.Agent("repair", flow.AgentSpec[agentState]{
		Policy: flow.AgentFunc[agentState](func(_ context.Context, s *agentState, history []flow.AgentTurn) (flow.AgentDecision, error) {
			if len(history) == 0 {
				return flow.AgentDecision{Tool: "fail"}, nil
			}
			s.History = append([]flow.AgentTurn(nil), history...)
			return flow.AgentDecision{Done: true}, nil
		}),
		Tools:         []flow.Tool[agentState]{failing},
		MaxIterations: 5,
	}).Next("done")
	wf.Succeed("done")

	var state agentState
	res := runOK(t, wf, &state)

	if len(state.History) != 1 || !strings.Contains(state.History[0].Error, "tool exploded") {
		t.Fatalf("history = %+v, want tool error", state.History)
	}
	if got := countEvents(res.Trace, flow.EventToolFailed); got != 1 {
		t.Fatalf("tool failed events = %d, want 1", got)
	}
}

func TestAgentIterationLimit(t *testing.T) {
	wf := flow.New[agentState]("agent-limit")
	wf.Agent("repair", flow.AgentSpec[agentState]{
		Policy: flow.AgentFunc[agentState](func(context.Context, *agentState, []flow.AgentTurn) (flow.AgentDecision, error) {
			return flow.AgentDecision{Tool: "add"}, nil
		}),
		Tools:         []flow.Tool[agentState]{addTool()},
		MaxIterations: 2,
	}).Next("done")
	wf.Succeed("done")

	var state agentState
	res, err := wf.Run(context.Background(), &state)
	if !errors.Is(err, flow.ErrAgentIterations) {
		t.Fatalf("error = %v, want ErrAgentIterations", err)
	}
	if res.Status != flow.StatusFailed {
		t.Fatalf("status = %q, want failed", res.Status)
	}
	if state.Value != 2 {
		t.Fatalf("Value = %d, want 2", state.Value)
	}
}

func TestAgentCanReportFailure(t *testing.T) {
	wf := flow.New[agentState]("agent-fail")
	wf.Agent("repair", flow.AgentSpec[agentState]{
		Policy: flow.AgentFunc[agentState](func(context.Context, *agentState, []flow.AgentTurn) (flow.AgentDecision, error) {
			return flow.AgentDecision{Fail: true, Message: "cannot fix this"}, nil
		}),
		MaxIterations: 2,
	}).Next("done")
	wf.Succeed("done")

	var state agentState
	res, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "cannot fix this") {
		t.Fatalf("error = %v, want agent message", err)
	}
	if res.Status != flow.StatusFailed {
		t.Fatalf("status = %q, want failed", res.Status)
	}
}

func TestMaxStepsProtectsLoops(t *testing.T) {
	wf := flow.New[seqState]("loop")
	wf.Task("loop", func(_ context.Context, s *seqState) error {
		s.Value++
		return nil
	}).Next("check")
	wf.Choice("check", func(_ context.Context, s *seqState) (string, error) {
		if s.Value < 3 {
			return "loop", nil
		}
		return "done", nil
	}).When("loop", "loop").When("done", "done")
	wf.Succeed("done")

	var state seqState
	res, err := wf.Run(context.Background(), &state, flow.WithMaxSteps(3))
	if !errors.Is(err, flow.ErrMaxSteps) {
		t.Fatalf("error = %v, want ErrMaxSteps", err)
	}
	if res.Steps != 3 {
		t.Fatalf("Steps = %d, want 3", res.Steps)
	}
	if state.Value != 2 {
		t.Fatalf("Value = %d, want 2", state.Value)
	}
}

func TestObserverSeesEveryEvent(t *testing.T) {
	wf := flow.New[seqState]("observe")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("done")
	wf.Succeed("done")

	var observed []flow.Event
	var state seqState
	res := runOK(t, wf, &state, flow.WithObserver(func(ev flow.Event) {
		observed = append(observed, ev)
	}))

	if len(observed) != len(res.Trace) {
		t.Fatalf("observed %d events, trace has %d", len(observed), len(res.Trace))
	}
	for i := range observed {
		if observed[i].Seq != res.Trace[i].Seq {
			t.Fatalf("event %d mismatch: %+v vs %+v", i, observed[i], res.Trace[i])
		}
	}
}

func TestMatchIs(t *testing.T) {
	boom := errors.New("boom")
	m := flow.MatchIs[seqState](boom)
	if !m(boom) {
		t.Fatal("MatchIs should match the same error")
	}
	if m(errors.New("boom")) {
		t.Fatal("MatchIs should not match a different error with the same message")
	}
}

func TestMatchAs(t *testing.T) {
	m := flow.MatchAs(&os.PathError{})

	err := &os.PathError{Op: "open", Path: "foo", Err: errors.New("boom")}
	if !m(err) {
		t.Fatal("MatchAs should match the target type")
	}
	if m(errors.New("boom")) {
		t.Fatal("MatchAs should not match an unrelated error")
	}
}

func TestParallelCancelsSiblingsOnFailure(t *testing.T) {
	boom := errors.New("branch b failed")
	startedA := atomic.Bool{}
	cancelledA := atomic.Bool{}

	wf := flow.New[struct{}]("parallel-cancel")
	wf.Parallel("fan",
		flow.Branch[struct{}]{
			Name: "a",
			Run: func(ctx context.Context, _ *struct{}) error {
				startedA.Store(true)
				select {
				case <-ctx.Done():
					cancelledA.Store(true)
					return ctx.Err()
				case <-time.After(time.Hour):
					return nil
				}
			},
		},
		flow.Branch[struct{}]{
			Name: "b",
			Run: func(context.Context, *struct{}) error {
				return boom
			},
		},
	).Next("done")
	wf.Succeed("done")

	var state struct{}
	_, err := wf.Run(context.Background(), &state)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if !startedA.Load() {
		t.Fatal("branch a did not start")
	}
	if !cancelledA.Load() {
		t.Fatal("branch a was not cancelled after sibling failed")
	}
}

func TestParallelIsolatedRunsOnCopies(t *testing.T) {
	// Each branch writes to its own copy of state; no mutex needed.
	copyState := func(s *seqState) *seqState {
		steps := make([]string, len(s.Steps))
		copy(steps, s.Steps)
		return &seqState{
			Value:    s.Value,
			Steps:    steps,
			Attempts: s.Attempts,
			Flag:     s.Flag,
		}
	}
	mergeState := func(main *seqState, copies []*seqState) error {
		for _, c := range copies {
			main.Value += c.Value
			main.Steps = append(main.Steps, c.Steps...)
			main.Attempts += c.Attempts
			main.Flag = main.Flag || c.Flag
		}
		return nil
	}

	wf := flow.New[seqState]("parallel-isolated")
	wf.ParallelIsolated("fan", copyState, mergeState,
		flow.Branch[seqState]{
			Name: "a",
			Run: func(_ context.Context, s *seqState) error {
				s.Value += 10
				s.Steps = append(s.Steps, "a")
				return nil
			},
		},
		flow.Branch[seqState]{
			Name: "b",
			Run: func(_ context.Context, s *seqState) error {
				s.Value += 20
				s.Steps = append(s.Steps, "b")
				s.Flag = true
				return nil
			},
		},
	).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	// Main state should not have been mutated by branches directly.
	if state.Value != 30 {
		t.Fatalf("Value = %d, want 30 (merged from copies)", state.Value)
	}
	if !state.Flag {
		t.Fatal("Flag should be merged from branch b")
	}
	if len(state.Steps) != 2 {
		t.Fatalf("Steps = %v, want [a b]", state.Steps)
	}
	if res.Steps != 2 {
		t.Fatalf("Steps = %d, want 2", res.Steps)
	}
}

func TestParallelIsolatedRequiresMerge(t *testing.T) {
	wf := flow.New[seqState]("parallel-no-merge")
	wf.ParallelIsolated("fan", func(s *seqState) *seqState { return s }, nil,
		flow.Branch[seqState]{
			Name: "a",
			Run:  func(context.Context, *seqState) error { return nil },
		},
	)

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "parallel copy requires a merge function") {
		t.Fatalf("error = %v, want merge required", err)
	}
}

func TestValidateDetectsUnreachableNodes(t *testing.T) {
	wf := flow.New[seqState]("unreachable")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("done")
	wf.Task("orphan", func(context.Context, *seqState) error { return nil }).Next("done")
	wf.Succeed("done")

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("error = %v, want unreachable node error", err)
	}
	if !strings.Contains(err.Error(), "orphan") {
		t.Fatalf("error = %v, want to mention orphan node", err)
	}
}

func TestValidateDetectsDeadLoop(t *testing.T) {
	wf := flow.New[seqState]("dead-loop")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("split")
	split := wf.Choice("split", func(context.Context, *seqState) (string, error) { return "bad", nil })
	split.When("good", "good").When("bad", "bad1")
	wf.Task("good", func(context.Context, *seqState) error { return nil }).Next("done")
	wf.Succeed("done")
	wf.Task("bad1", func(context.Context, *seqState) error { return nil }).Next("bad2")
	wf.Task("bad2", func(context.Context, *seqState) error { return nil }).Next("bad1")

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "cannot reach any terminal") {
		t.Fatalf("error = %v, want dead loop error", err)
	}
}

func TestValidateDetectsMissingTerminal(t *testing.T) {
	wf := flow.New[seqState]("no-terminal")
	wf.Task("a", func(context.Context, *seqState) error { return nil }).Next("b")
	wf.Task("b", func(context.Context, *seqState) error { return nil }).Next("a")

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "no terminal node") {
		t.Fatalf("error = %v, want missing terminal error", err)
	}
}

func TestValidateGraphStructureAllowsValidGraph(t *testing.T) {
	wf := flow.New[seqState]("valid")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("choice")
	wf.Choice("choice", func(context.Context, *seqState) (string, error) { return "done", nil }).
		When("done", "done")
	wf.Succeed("done")

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err != nil {
		t.Fatalf("error = %v, want no error", err)
	}
}

func TestFailNodeReturnsBusinessFailedStatus(t *testing.T) {
	wf := flow.New[seqState]("explicit-fail")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("give-up")
	wf.Fail("give-up", "deliberate stop")

	var state seqState
	res, err := wf.Run(context.Background(), &state)
	if err == nil {
		t.Fatal("expected error from Fail node")
	}
	if res.Status != flow.StatusBusinessFailed {
		t.Fatalf("status = %q, want %q", res.Status, flow.StatusBusinessFailed)
	}
	if !strings.Contains(err.Error(), "deliberate stop") {
		t.Fatalf("error = %v, want deliberate stop", err)
	}
}

func TestValidateRejectsBadGraphs(t *testing.T) {
	tests := []struct {
		name   string
		build  func() *flow.Workflow[seqState]
		expect string
	}{
		{
			name: "missing next",
			build: func() *flow.Workflow[seqState] {
				wf := flow.New[seqState]("bad")
				wf.Task("task", func(context.Context, *seqState) error { return nil })
				return wf
			},
			expect: "Next is not set",
		},
		{
			name: "missing target",
			build: func() *flow.Workflow[seqState] {
				wf := flow.New[seqState]("bad")
				wf.Task("task", func(context.Context, *seqState) error { return nil }).Next("nowhere")
				return wf
			},
			expect: "does not exist",
		},
		{
			name: "nil task",
			build: func() *flow.Workflow[seqState] {
				wf := flow.New[seqState]("bad")
				wf.Task("task", nil).Next("done")
				wf.Succeed("done")
				return wf
			},
			expect: "task function is nil",
		},
		{
			name: "choice without routes",
			build: func() *flow.Workflow[seqState] {
				wf := flow.New[seqState]("bad")
				wf.Choice("choice", func(context.Context, *seqState) (string, error) { return "x", nil })
				return wf
			},
			expect: "choice has no routes",
		},
		{
			name: "decision without policy",
			build: func() *flow.Workflow[seqState] {
				wf := flow.New[seqState]("bad")
				wf.Decision("decision", flow.DecisionSpec[seqState]{
					Options: []flow.RouteOption{{Key: "x", Target: "done"}},
				})
				wf.Succeed("done")
				return wf
			},
			expect: "decision policy is nil",
		},
		{
			name: "terminal has next",
			build: func() *flow.Workflow[seqState] {
				wf := flow.New[seqState]("bad")
				wf.Succeed("done").Next("done")
				return wf
			},
			expect: "terminal node must not have Next",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state seqState
			_, err := tt.build().Run(context.Background(), &state)
			if err == nil || !strings.Contains(err.Error(), tt.expect) {
				t.Fatalf("error = %v, want %q", err, tt.expect)
			}
		})
	}
}

func TestMermaidRendering(t *testing.T) {
	wf := flow.New[seqState]("all-nodes")
	wf.Task("load", func(context.Context, *seqState) error { return nil }).
		WithRetry(flow.RetryPolicy{MaxAttempts: 2, InitialInterval: time.Millisecond}).
		Catch("fail", nil).
		Next("triage")
	wf.Decision("triage", flow.DecisionSpec[seqState]{
		Options: []flow.RouteOption{
			{Key: "fix", Description: "repair the code", Target: "gate"},
			{Key: "skip", Description: "stop", Target: "fail"},
		},
		Choose: flow.DecisionFunc[seqState](func(context.Context, *seqState, []flow.RouteOption) (string, error) {
			return "fix", nil
		}),
	})
	wf.Choice("gate", func(context.Context, *seqState) (string, error) {
		return "ok", nil
	}).When("ok", "fan").Otherwise("fail")
	wf.Parallel("fan",
		flow.Branch[seqState]{Name: "build", Run: func(context.Context, *seqState) error { return nil }},
		flow.Branch[seqState]{Name: "test", Run: func(context.Context, *seqState) error { return nil }},
	).Next("slow")
	wf.Wait("slow", time.Second).Next("repair")
	wf.Agent("repair", flow.AgentSpec[seqState]{
		Policy: flow.AgentFunc[seqState](func(context.Context, *seqState, []flow.AgentTurn) (flow.AgentDecision, error) {
			return flow.AgentDecision{Done: true}, nil
		}),
		Tools: []flow.Tool[seqState]{
			{Name: "run_tests", Run: func(context.Context, *seqState, map[string]string) (string, error) {
				return "ok", nil
			}},
		},
	}).Next("done")
	wf.Succeed("done")
	wf.Fail("fail", "gave up")

	first := wf.Mermaid()
	second := wf.Mermaid()
	if first != second {
		t.Fatal("Mermaid output is not deterministic")
	}

	checks := []string{
		"flowchart TD",
		`start(["start"])`,
		`start --> n0`,
		`n0["load<br/><small>task / retry x2</small>"]`,
		`n1{{"triage<br/><small>decision / 2 options</small>"}}`,
		`n2{"gate<br/><small>choice</small>"}`,
		`n4["slow<br/><small>wait 1s</small>"]`,
		`n5["repair<br/><small>agent loop / 1 tools</small>"]`,
		`n1 -->|"fix"| n2`,
		`n1 -->|"skip"| n7`,
		`n2 -->|"ok"| n3`,
		`n2 -->|"otherwise"| n7`,
		`subgraph n3_par["parallel branches"]`,
		`n3_b0["branch: build"]`,
		`n3 --> n3_b0`,
		`n3_b0 --> n4`,
		`n4 --> n5`,
		`n5 --> n6`,
		`n0 -.->|"error"| n7`,
		`classDef decision`,
	}
	for _, want := range checks {
		if !strings.Contains(first, want) {
			t.Errorf("Mermaid output missing %q\n---\n%s", want, first)
		}
	}
}

func TestSubWorkflowRunsAndCompletes(t *testing.T) {
	// Build a sub-workflow that increments a counter.
	sub := flow.New[seqState]("sub")
	sub.Task("inc", func(_ context.Context, s *seqState) error {
		s.Value++
		return nil
	}).Next("done")
	sub.Succeed("done")

	// Parent workflow calls the sub-workflow as a single node.
	wf := flow.New[seqState]("parent")
	wf.Task("setup", func(_ context.Context, s *seqState) error {
		s.Value = 0
		return nil
	}).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("report")
	wf.Task("report", func(_ context.Context, s *seqState) error {
		return nil
	}).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if state.Value != 1 {
		t.Fatalf("Value = %d, want 1", state.Value)
	}
	if res.Steps != 4 {
		t.Fatalf("Steps = %d, want 4 (setup, sub-workflow, report, done)", res.Steps)
	}
}

func TestSubWorkflowPropagatesErrorWithTrace(t *testing.T) {
	boom := errors.New("sub-workflow failure")

	// Sub-workflow that fails.
	sub := flow.New[seqState]("failing-sub")
	sub.Task("bad", func(context.Context, *seqState) error {
		return boom
	}).Next("done")
	sub.Succeed("done")

	// Parent without Catch: error propagates with trace attached.
	wf := flow.New[seqState]("parent")
	wf.Task("setup", func(context.Context, *seqState) error { return nil }).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("report")
	wf.Task("report", func(context.Context, *seqState) error { return nil }).Next("done")
	wf.Succeed("done")

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err == nil {
		t.Fatal("expected error from failing sub-workflow")
	}

	var swe *flow.SubWorkflowError
	if !errors.As(err, &swe) {
		t.Fatalf("error = %v, want *flow.SubWorkflowError", err)
	}
	if len(swe.Trace) == 0 {
		t.Fatal("SubWorkflowError should have non-empty trace")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want errors.Is(boom) = true", err)
	}

	// Parent with Catch: sub-workflow error is handled by error route.
	wf2 := flow.New[seqState]("parent-with-catch")
	wf2.Task("setup", func(context.Context, *seqState) error { return nil }).Next("run-sub")
	wf2.SubWorkflow("run-sub", sub).
		Catch("recover", flow.MatchAs(&flow.SubWorkflowError{})).
		Next("report")
	wf2.Task("recover", func(_ context.Context, s *seqState) error {
		s.Flag = true
		return nil
	}).Next("report")
	wf2.Task("report", func(context.Context, *seqState) error { return nil }).Next("done")
	wf2.Succeed("done")

	var state2 seqState
	res := runOK(t, wf2, &state2)
	if res.Status != flow.StatusSucceeded {
		t.Fatalf("status = %q, want succeeded", res.Status)
	}
	if !state2.Flag {
		t.Fatal("recovery task did not run")
	}
}

func TestSubWorkflowNested(t *testing.T) {
	// Innermost sub-workflow.
	inner := flow.New[seqState]("inner")
	inner.Task("inner-task", func(_ context.Context, s *seqState) error {
		s.Value++
		return nil
	}).Next("inner-done")
	inner.Succeed("inner-done")

	// Middle sub-workflow contains the inner one.
	middle := flow.New[seqState]("middle")
	middle.Task("mid-prep", func(context.Context, *seqState) error { return nil }).Next("run-inner")
	middle.SubWorkflow("run-inner", inner).Next("mid-done")
	middle.Succeed("mid-done")

	// Parent contains the middle sub-workflow.
	wf := flow.New[seqState]("parent")
	wf.Task("start", func(_ context.Context, s *seqState) error {
		s.Value = 0
		return nil
	}).Next("run-middle")
	wf.SubWorkflow("run-middle", middle).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if state.Value != 1 {
		t.Fatalf("Value = %d, want 1", state.Value)
	}
	if res.Steps != 3 {
		t.Fatalf("Steps = %d, want 3 (start, sub-workflow, done)", res.Steps)
	}
}

func TestSubWorkflowValidation(t *testing.T) {
	// Nil sub-workflow should fail validation.
	wf := flow.New[seqState]("bad")
	wf.SubWorkflow("sub", nil).Next("done")
	wf.Succeed("done")

	var state seqState
	_, err := wf.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "sub-workflow is nil") {
		t.Fatalf("error = %v, want 'sub-workflow is nil'", err)
	}

	// Invalid sub-workflow (missing Next) should fail parent validation.
	sub := flow.New[seqState]("invalid-sub")
	sub.Task("task", func(context.Context, *seqState) error { return nil })
	// No Next on the task - validation will catch it

	wf2 := flow.New[seqState]("parent2")
	wf2.SubWorkflow("sub", sub).Next("done")
	wf2.Succeed("done")

	_, err = wf2.Run(context.Background(), &state)
	if err == nil || !strings.Contains(err.Error(), "Next is not set") {
		t.Fatalf("error = %v, want 'Next is not set'", err)
	}
}

func TestSubWorkflowEmitsSingleLifecycleEvents(t *testing.T) {
	sub := flow.New[seqState]("sub")
	sub.Task("inc", func(_ context.Context, s *seqState) error {
		s.Value++
		return nil
	}).Next("sub-done")
	sub.Succeed("sub-done")

	wf := flow.New[seqState]("parent")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("done")
	wf.Succeed("done")

	var state seqState
	res := runOK(t, wf, &state)

	if got := countNodeEvents(res.Trace, flow.EventNodeStarted, "run-sub"); got != 1 {
		t.Fatalf("node_started events for run-sub = %d, want 1\n%v", got, res.Trace)
	}
	if got := countNodeEvents(res.Trace, flow.EventNodeFinished, "run-sub"); got != 1 {
		t.Fatalf("node_finished events for run-sub = %d, want 1\n%v", got, res.Trace)
	}
	if got := countNodeEvents(res.Trace, flow.EventNodeFailed, "run-sub"); got != 0 {
		t.Fatalf("node_failed events for run-sub = %d, want 0\n%v", got, res.Trace)
	}
}

func TestSubWorkflowFailureEmitsSingleLifecycleEvents(t *testing.T) {
	boom := errors.New("sub-workflow failure")
	sub := flow.New[seqState]("sub")
	sub.Task("bad", func(context.Context, *seqState) error { return boom }).Next("sub-done")
	sub.Succeed("sub-done")

	wf := flow.New[seqState]("parent")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("done")
	wf.Succeed("done")

	var state seqState
	res, err := wf.Run(context.Background(), &state)
	if err == nil {
		t.Fatal("expected error from failing sub-workflow")
	}
	if res == nil {
		t.Fatal("Run returned a nil result")
	}
	if got := countNodeEvents(res.Trace, flow.EventNodeStarted, "run-sub"); got != 1 {
		t.Fatalf("node_started events for run-sub = %d, want 1\n%v", got, res.Trace)
	}
	if got := countNodeEvents(res.Trace, flow.EventNodeFailed, "run-sub"); got != 1 {
		t.Fatalf("node_failed events for run-sub = %d, want 1\n%v", got, res.Trace)
	}
	if got := countNodeEvents(res.Trace, flow.EventNodeFinished, "run-sub"); got != 0 {
		t.Fatalf("node_finished events for run-sub = %d, want 0\n%v", got, res.Trace)
	}
}

func TestSubWorkflowValidationRejectsCycles(t *testing.T) {
	t.Run("mutual", func(t *testing.T) {
		a := flow.New[seqState]("a")
		b := flow.New[seqState]("b")
		a.SubWorkflow("to-b", b).Next("a-done")
		a.Succeed("a-done")
		b.SubWorkflow("to-a", a).Next("b-done")
		b.Succeed("b-done")

		err := a.Validate()
		if err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("Validate error = %v, want a cycle error", err)
		}
	})

	t.Run("self", func(t *testing.T) {
		wf := flow.New[seqState]("self")
		wf.SubWorkflow("loop", wf).Next("done")
		wf.Succeed("done")

		err := wf.Validate()
		if err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("Validate error = %v, want a cycle error", err)
		}
	})
}

func TestSubWorkflowValidationAllowsSharedSubWorkflow(t *testing.T) {
	sub := flow.New[seqState]("shared")
	sub.Task("work", func(context.Context, *seqState) error { return nil }).Next("sub-done")
	sub.Succeed("sub-done")

	wf := flow.New[seqState]("parent")
	wf.SubWorkflow("one", sub).Next("two")
	wf.SubWorkflow("two", sub).Next("done")
	wf.Succeed("done")

	if err := wf.Validate(); err != nil {
		t.Fatalf("Validate returned error for shared sub-workflow: %v", err)
	}
}

func TestMermaidRenderingSubWorkflow(t *testing.T) {
	sub := flow.New[seqState]("sub")
	sub.Task("sub-task", func(context.Context, *seqState) error { return nil }).Next("sub-done")
	sub.Succeed("sub-done")

	wf := flow.New[seqState]("parent")
	wf.Task("start", func(context.Context, *seqState) error { return nil }).Next("run-sub")
	wf.SubWorkflow("run-sub", sub).Next("done")
	wf.Succeed("done")

	out := wf.Mermaid()
	if !strings.Contains(out, `n1[["run-sub<br/><small>sub-workflow / 2 nodes</small>"]]`) {
		t.Errorf("Mermaid output missing sub-workflow node\n---\n%s", out)
	}
	if !strings.Contains(out, `classDef subworkflow`) {
		t.Errorf("Mermaid output missing subworkflow classDef\n---\n%s", out)
	}
}
