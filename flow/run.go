package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Status is the final state of a workflow run.
type Status string

const (
	// StatusSucceeded means the workflow reached a Succeed node.
	StatusSucceeded Status = "succeeded"
	// StatusFailed means a node failed and no error route handled it.
	StatusFailed Status = "failed"
	// StatusCanceled means the caller's context ended the run.
	StatusCanceled Status = "canceled"
)

var (
	// ErrMaxSteps is returned when a run exceeds its node-visit budget.
	ErrMaxSteps = errors.New("workflow exceeded maximum steps")
	// ErrAgentIterations is returned when an agent loop exceeds its budget.
	ErrAgentIterations = errors.New("agent exceeded maximum iterations")
)

// EventKind identifies an execution trace event.
type EventKind string

const (
	EventNodeStarted      EventKind = "node_started"
	EventNodeFinished     EventKind = "node_finished"
	EventNodeFailed       EventKind = "node_failed"
	EventRetry            EventKind = "retry"
	EventBranchStarted    EventKind = "branch_started"
	EventBranchFinished   EventKind = "branch_finished"
	EventBranchFailed     EventKind = "branch_failed"
	EventRouteSelected    EventKind = "route_selected"
	EventDecisionRejected EventKind = "decision_rejected"
	EventAgentThought     EventKind = "agent_thought"
	EventToolStarted      EventKind = "tool_started"
	EventToolFinished     EventKind = "tool_finished"
	EventToolFailed       EventKind = "tool_failed"
	EventToolRejected     EventKind = "tool_rejected"
	EventAgentFinished    EventKind = "agent_finished"
	EventWorkflowFinished EventKind = "workflow_finished"
	EventWorkflowFailed   EventKind = "workflow_failed"
	EventWorkflowCanceled EventKind = "workflow_canceled"
)

// Event is one entry in an execution trace. It is deliberately flat so traces
// are easy to serialize, compare in tests and print.
type Event struct {
	Seq     int
	Time    time.Time
	Kind    EventKind
	Node    string
	Branch  string
	Step    int
	Attempt int
	Route   string
	Tool    string
	Message string
}

// Result describes a completed run. State always points at the caller's state.
type Result[T any] struct {
	Workflow string
	Status   Status
	State    *T
	Trace    []Event
	Steps    int
	LastNode string
	Err      error
}

// RunOption configures a single execution.
type RunOption func(*runConfig)

type runConfig struct {
	maxSteps int
	observer func(Event)
}

// WithMaxSteps overrides the node-visit budget. Values below 1 are ignored.
func WithMaxSteps(n int) RunOption {
	return func(c *runConfig) {
		if n > 0 {
			c.maxSteps = n
		}
	}
}

// WithObserver receives every trace event as it happens. The observer is called
// from the goroutine driving the run, never from parallel branches, so it does
// not need to be concurrency-safe.
func WithObserver(fn func(Event)) RunOption {
	return func(c *runConfig) {
		c.observer = fn
	}
}

// Run executes the workflow against state. It validates the graph first and
// returns the run result together with the terminal error, if any.
func (w *Workflow[T]) Run(ctx context.Context, state *T, opts ...RunOption) (*Result[T], error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}

	cfg := runConfig{maxSteps: DefaultMaxSteps}
	for _, opt := range opts {
		opt(&cfg)
	}

	res := &Result[T]{
		Workflow: w.Name,
		Status:   StatusFailed,
		State:    state,
	}
	seq := 0
	emit := func(ev Event) {
		seq++
		ev.Seq = seq
		ev.Time = time.Now()
		res.Trace = append(res.Trace, ev)
		if cfg.observer != nil {
			cfg.observer(ev)
		}
	}
	finish := func(status Status, err error, node string) (*Result[T], error) {
		res.Status = status
		res.Err = err
		res.LastNode = node
		switch status {
		case StatusSucceeded:
			emit(Event{Kind: EventWorkflowFinished, Node: node, Message: "workflow succeeded"})
		case StatusCanceled:
			msg := "workflow canceled"
			if err != nil {
				msg = err.Error()
			}
			emit(Event{Kind: EventWorkflowCanceled, Node: node, Message: msg})
		default:
			msg := "workflow failed"
			if err != nil {
				msg = err.Error()
			}
			emit(Event{Kind: EventWorkflowFailed, Node: node, Message: msg})
		}
		return res, err
	}

	current := w.start
	step := 0
	for {
		if err := ctx.Err(); err != nil {
			res.Steps = step
			return finish(StatusCanceled, err, current)
		}
		if step >= cfg.maxSteps {
			res.Steps = step
			return finish(StatusFailed, fmt.Errorf("%w (limit %d)", ErrMaxSteps, cfg.maxSteps), current)
		}

		node, ok := w.nodes[current]
		if !ok {
			res.Steps = step
			return finish(StatusFailed, fmt.Errorf("node %q does not exist", current), current)
		}

		step++
		res.Steps = step
		emit(Event{Kind: EventNodeStarted, Node: node.Name, Step: step})

		next, err := w.executeNode(ctx, node, state, step, emit)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				res.Steps = step
				return finish(StatusCanceled, ctxErr, node.Name)
			}
			emit(Event{Kind: EventNodeFailed, Node: node.Name, Step: step, Message: err.Error()})
			if target, handled := matchErrorRoute(node, err); handled {
				emit(Event{
					Kind:    EventRouteSelected,
					Node:    node.Name,
					Step:    step,
					Route:   "error",
					Message: err.Error(),
				})
				current = target
				continue
			}
			res.Steps = step
			return finish(StatusFailed, fmt.Errorf("node %q: %w", node.Name, err), node.Name)
		}

		if node.Kind == KindFail {
			message := node.FailMessage
			if message == "" {
				message = "workflow failed"
			}
			emit(Event{Kind: EventNodeFailed, Node: node.Name, Step: step, Message: message})
			res.Steps = step
			return finish(StatusFailed, fmt.Errorf("node %q: %s", node.Name, message), node.Name)
		}

		emit(Event{Kind: EventNodeFinished, Node: node.Name, Step: step})

		if node.Kind == KindSucceed {
			res.Steps = step
			return finish(StatusSucceeded, nil, node.Name)
		}
		if next == "" {
			res.Steps = step
			return finish(StatusFailed, fmt.Errorf("node %q has no next node", node.Name), node.Name)
		}
		current = next
	}
}

func (w *Workflow[T]) executeNode(
	ctx context.Context,
	node *Node[T],
	state *T,
	step int,
	emit func(Event),
) (string, error) {
	switch node.Kind {
	case KindTask:
		if err := runTask(ctx, node, state, step, emit); err != nil {
			return "", err
		}
		return node.NextNode, nil

	case KindChoice:
		return runChoice(ctx, node, state, step, emit)

	case KindDecision:
		return runDecision(ctx, node, state, step, emit)

	case KindParallel:
		if err := runParallel(ctx, node, state, step, emit); err != nil {
			return "", err
		}
		return node.NextNode, nil

	case KindWait:
		if err := runWait(ctx, node); err != nil {
			return "", err
		}
		return node.NextNode, nil

	case KindAgent:
		if err := runAgent(ctx, node, state, step, emit); err != nil {
			return "", err
		}
		return node.NextNode, nil

	case KindSucceed, KindFail:
		return "", nil
	}
	return "", fmt.Errorf("unknown node kind %q", node.Kind)
}

func runTask[T any](
	ctx context.Context,
	node *Node[T],
	state *T,
	step int,
	emit func(Event),
) error {
	attempts := 1
	if node.Retry != nil {
		attempts = node.Retry.MaxAttempts
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		lastErr = node.Task(ctx, state)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == attempts {
			break
		}
		emit(Event{
			Kind:    EventRetry,
			Node:    node.Name,
			Step:    step,
			Attempt: attempt + 1,
			Message: lastErr.Error(),
		})
		if delay := retryDelay(node.Retry, attempt); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func retryDelay(policy *RetryPolicy, failedAttempt int) time.Duration {
	if policy == nil {
		return 0
	}
	delay := policy.InitialInterval
	backoff := policy.Backoff
	if backoff < 1 {
		backoff = 1
	}
	for i := 1; i < failedAttempt; i++ {
		delay = time.Duration(float64(delay) * backoff)
		if policy.MaxInterval > 0 && delay > policy.MaxInterval {
			delay = policy.MaxInterval
		}
	}
	if policy.MaxInterval > 0 && delay > policy.MaxInterval {
		delay = policy.MaxInterval
	}
	return delay
}

func runChoice[T any](
	ctx context.Context,
	node *Node[T],
	state *T,
	step int,
	emit func(Event),
) (string, error) {
	key, err := node.Choice(ctx, state)
	if err != nil {
		return "", err
	}
	if key == "" {
		key = DefaultRoute
	}
	target, ok := node.Routes[key]
	if !ok {
		return "", fmt.Errorf("choice returned unknown route %q (declared: %v)", key, node.RouteKeys)
	}
	emit(Event{Kind: EventRouteSelected, Node: node.Name, Step: step, Route: key})
	return target, nil
}

func runDecision[T any](
	ctx context.Context,
	node *Node[T],
	state *T,
	step int,
	emit func(Event),
) (string, error) {
	spec := node.Decision
	attempts := spec.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		key, err := spec.Choose.Choose(ctx, state, spec.Options)
		if err != nil {
			return "", err
		}
		target, ok := node.Routes[key]
		if !ok {
			lastErr = fmt.Errorf("decision returned unknown option %q (declared: %v)", key, node.RouteKeys)
			emit(Event{
				Kind:    EventDecisionRejected,
				Node:    node.Name,
				Step:    step,
				Attempt: attempt,
				Route:   key,
				Message: lastErr.Error(),
			})
			continue
		}
		emit(Event{
			Kind:    EventRouteSelected,
			Node:    node.Name,
			Step:    step,
			Attempt: attempt,
			Route:   key,
		})
		return target, nil
	}
	return "", lastErr
}

func runParallel[T any](
	ctx context.Context,
	node *Node[T],
	state *T,
	step int,
	emit func(Event),
) error {
	// Branches share state by design. Callers are responsible for any locking
	// needed by concurrent mutations.
	branchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(node.Branches))
	var wg sync.WaitGroup
	for i := range node.Branches {
		branch := node.Branches[i]
		emit(Event{Kind: EventBranchStarted, Node: node.Name, Branch: branch.Name, Step: step})
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = node.Branches[i].Run(branchCtx, state)
		}(i)
	}
	wg.Wait()

	for i, branch := range node.Branches {
		if errs[i] != nil {
			emit(Event{
				Kind:    EventBranchFailed,
				Node:    node.Name,
				Branch:  branch.Name,
				Step:    step,
				Message: errs[i].Error(),
			})
			continue
		}
		emit(Event{Kind: EventBranchFinished, Node: node.Name, Branch: branch.Name, Step: step})
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func runWait[T any](ctx context.Context, node *Node[T]) error {
	if node.Delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(node.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runAgent[T any](
	ctx context.Context,
	node *Node[T],
	state *T,
	step int,
	emit func(Event),
) error {
	spec := node.Agent
	iterations := spec.MaxIterations
	if iterations == 0 {
		iterations = DefaultMaxAgentIterations
	}

	tools := make(map[string]Tool[T], len(spec.Tools))
	for _, tool := range spec.Tools {
		tools[tool.Name] = tool
	}

	history := make([]AgentTurn, 0, iterations)
	for i := 1; i <= iterations; i++ {
		decision, err := spec.Policy.Next(ctx, state, history)
		if err != nil {
			return err
		}
		if decision.Thought != "" {
			emit(Event{
				Kind:    EventAgentThought,
				Node:    node.Name,
				Step:    step,
				Attempt: i,
				Message: decision.Thought,
			})
		}
		if decision.Done && decision.Fail {
			return fmt.Errorf("agent returned both Done and Fail")
		}
		if decision.Fail {
			message := decision.Message
			if message == "" {
				message = "agent reported failure"
			}
			return errors.New(message)
		}
		if decision.Done {
			emit(Event{
				Kind:    EventAgentFinished,
				Node:    node.Name,
				Step:    step,
				Attempt: i,
				Message: decision.Message,
			})
			return nil
		}

		turn := AgentTurn{
			Step:    i,
			Thought: decision.Thought,
			Tool:    decision.Tool,
			Args:    decision.Args,
		}
		if decision.Tool == "" {
			turn.Error = "policy returned no tool and Done=false"
			history = append(history, turn)
			emit(Event{
				Kind:    EventToolRejected,
				Node:    node.Name,
				Step:    step,
				Attempt: i,
				Message: turn.Error,
			})
			continue
		}

		tool, ok := tools[decision.Tool]
		if !ok {
			turn.Error = fmt.Sprintf("unknown tool %q", decision.Tool)
			history = append(history, turn)
			emit(Event{
				Kind:    EventToolRejected,
				Node:    node.Name,
				Step:    step,
				Attempt: i,
				Tool:    decision.Tool,
				Message: turn.Error,
			})
			continue
		}

		emit(Event{
			Kind:    EventToolStarted,
			Node:    node.Name,
			Step:    step,
			Attempt: i,
			Tool:    decision.Tool,
		})
		observation, err := tool.Run(ctx, state, decision.Args)
		turn.Observation = observation
		if err != nil {
			turn.Error = err.Error()
			emit(Event{
				Kind:    EventToolFailed,
				Node:    node.Name,
				Step:    step,
				Attempt: i,
				Tool:    decision.Tool,
				Message: err.Error(),
			})
		} else {
			emit(Event{
				Kind:    EventToolFinished,
				Node:    node.Name,
				Step:    step,
				Attempt: i,
				Tool:    decision.Tool,
				Message: observation,
			})
		}
		history = append(history, turn)
	}
	return fmt.Errorf("%w: node %q hit limit %d", ErrAgentIterations, node.Name, iterations)
}

func matchErrorRoute[T any](node *Node[T], err error) (string, bool) {
	for _, route := range node.ErrorRoutes {
		if route.Match == nil || route.Match(err) {
			return route.Target, true
		}
	}
	return "", false
}
