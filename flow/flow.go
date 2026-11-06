// Package flow provides a small, in-process workflow engine for programs that
// need a fixed, visualizable control-flow graph with LLM-driven decisions.
//
// A workflow is built from typed nodes (Task, Choice, Decision, Parallel, Wait,
// Agent, Succeed, Fail) that all operate on a single state value. The graph is
// static: an LLM may choose among declared routes or tool calls, but it cannot
// invent new graph structure. That keeps runs inspectable and makes the Mermaid
// export an accurate picture of what can happen.
//
// The engine is intentionally not durable. It keeps no checkpoints and performs
// no replay; a run lives entirely in memory and is discarded when Run returns.
package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// NodeKind identifies the behaviour of a node.
type NodeKind string

const (
	// KindTask runs a function once, with optional retries.
	KindTask NodeKind = "task"
	// KindChoice is a deterministic branch on a route key.
	KindChoice NodeKind = "choice"
	// KindDecision delegates a validated route choice to a policy, typically an
	// LLM. The policy may only choose from the node's declared options.
	KindDecision NodeKind = "decision"
	// KindParallel runs independent branches concurrently and waits for all of
	// them, then continues at Next.
	KindParallel NodeKind = "parallel"
	// KindWait pauses for a fixed duration.
	KindWait NodeKind = "wait"
	// KindAgent runs an iterative tool-selection loop driven by a policy.
	KindAgent NodeKind = "agent"
	// KindSubWorkflow runs a nested workflow as a single node.
	KindSubWorkflow NodeKind = "sub-workflow"
	// KindSucceed ends the workflow successfully.
	KindSucceed NodeKind = "succeed"
	// KindFail ends the workflow with an error.
	KindFail NodeKind = "fail"
)

// DefaultRoute is the route key used by Choice.Otherwise and may also be used
// as an explicit Decision option key.
const DefaultRoute = "default"

// DefaultMaxSteps bounds the number of node visits in a single run. Loops are
// allowed, so this is the primary protection against a runaway graph.
const DefaultMaxSteps = 1000

// DefaultMaxAgentIterations bounds an Agent node when MaxIterations is zero.
const DefaultMaxAgentIterations = 8

// TaskFunc is the body of a Task node. It may mutate state directly.
type TaskFunc[T any] func(context.Context, *T) error

// ChoiceFunc decides which declared route a Choice node takes. Returning ""
// selects the DefaultRoute when one was installed with Otherwise.
type ChoiceFunc[T any] func(context.Context, *T) (string, error)

// RetryPolicy controls retries for a Task node.
type RetryPolicy struct {
	// MaxAttempts includes the first attempt, so 1 means no retry.
	MaxAttempts int
	// InitialInterval is the delay after the first failed attempt.
	InitialInterval time.Duration
	// Backoff multiplies the delay after each further failure. Values below 1
	// are treated as 1.
	Backoff float64
	// MaxInterval caps the delay. Zero means no cap.
	MaxInterval time.Duration
}

// ErrorRoute sends a node failure to another node instead of ending the run.
type ErrorRoute struct {
	// Target is the node to run when Match accepts the error.
	Target string
	// Match decides whether this route applies. A nil Match accepts any error.
	Match func(error) bool
}

// SubWorkflowError wraps a sub-workflow failure and attaches its execution
// trace so the parent can debug what happened inside. It implements
// errors.Is and errors.As so Catch routes with flow.MatchIs/flow.MatchAs
// can match against it.
type SubWorkflowError struct {
	// Err is the terminal error returned by the sub-workflow.
	Err error
	// Trace is the sub-workflow's execution trace, attached for debugging.
	Trace []Event
}

func (e *SubWorkflowError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "sub-workflow failed"
}

// Unwrap allows errors.Is and errors.As to look through SubWorkflowError.
func (e *SubWorkflowError) Unwrap() error { return e.Err }

// RouteOption is a decision the policy is allowed to make. Target is the node
// the workflow follows when the option is chosen.
type RouteOption struct {
	Key         string
	Description string
	Target      string
}

// MatchIs returns an ErrorRoute match function that matches errors using
// errors.Is against target. Use it for sentinel errors:
//
//	node.Catch("recover", flow.MatchIs(ErrRepoLocked))
func MatchIs[T any](target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}

// MatchAs returns an ErrorRoute match function that matches errors using
// errors.As semantics into target. Use it for custom error types:
//
//	node.Catch("recover", flow.MatchAs(&fsErr{}))
//
// The target is only used to establish the error type; its value is
// ignored. Pass a pointer to the error type you want to match, e.g.
// flow.MatchAs(&MyErr{}). The match succeeds when err's chain contains an
// error whose type is assignable to *T.
func MatchAs[T any](target *T) func(error) bool {
	targetType := reflect.TypeOf(target) // *T
	return func(err error) bool {
		for err != nil {
			if reflect.TypeOf(err).AssignableTo(targetType) {
				return true
			}
			err = errors.Unwrap(err)
		}
		return false
	}
}

// DecisionPolicy chooses one of the declared options. Implementations usually
// call an LLM and parse its structured response. The engine validates the
// returned key before following it.
type DecisionPolicy[T any] interface {
	Choose(ctx context.Context, state *T, options []RouteOption) (string, error)
}

// DecisionFunc adapts a function to DecisionPolicy.
type DecisionFunc[T any] func(context.Context, *T, []RouteOption) (string, error)

// Choose implements DecisionPolicy.
func (f DecisionFunc[T]) Choose(ctx context.Context, state *T, options []RouteOption) (string, error) {
	return f(ctx, state, options)
}

// DecisionSpec configures a Decision node.
type DecisionSpec[T any] struct {
	Options []RouteOption
	Choose  DecisionPolicy[T]
	// MaxAttempts is the number of times the policy is asked when it returns
	// an unknown option. Zero means one attempt.
	MaxAttempts int
}

// Tool is a capability exposed to an Agent policy. Tools receive a string map
// so a policy can emit simple structured arguments.
type Tool[T any] struct {
	Name        string
	Description string
	Run         func(ctx context.Context, state *T, args map[string]string) (string, error)
}

// AgentTurn is one entry in the transcript shown back to an Agent policy.
type AgentTurn struct {
	Step        int
	Thought     string
	Tool        string
	Args        map[string]string
	Observation string
	Error       string
}

// AgentDecision is the response from an Agent policy for one iteration.
//
// Exactly one mode should be used: Done ends the loop successfully, Fail ends
// it with an error, and otherwise Tool names the tool to call. A decision with
// neither a tool nor a terminal state is reported back to the policy as a
// malformed turn so it can correct itself.
type AgentDecision struct {
	Thought string
	Tool    string
	Args    map[string]string
	Done    bool
	Fail    bool
	Message string
}

// AgentPolicy selects the next tool call, or declares the loop finished.
type AgentPolicy[T any] interface {
	Next(ctx context.Context, state *T, history []AgentTurn) (AgentDecision, error)
}

// AgentFunc adapts a function to AgentPolicy.
type AgentFunc[T any] func(context.Context, *T, []AgentTurn) (AgentDecision, error)

// Next implements AgentPolicy.
func (f AgentFunc[T]) Next(ctx context.Context, state *T, history []AgentTurn) (AgentDecision, error) {
	return f(ctx, state, history)
}

// AgentSpec configures an Agent node.
type AgentSpec[T any] struct {
	// Goal is descriptive only and is useful in traces and prompts.
	Goal   string
	Policy AgentPolicy[T]
	Tools  []Tool[T]
	// MaxIterations bounds policy/tool turns. Zero means
	// DefaultMaxAgentIterations.
	MaxIterations int
}

// Branch is one concurrent arm of a Parallel node. Branches share the workflow
// state, so concurrent branches must not mutate the same fields without their
// own synchronisation.
type Branch[T any] struct {
	Name string
	Run  func(context.Context, *T) error
}

// Node is a single vertex in a workflow.
type Node[T any] struct {
	// Name uniquely identifies the node in its workflow.
	Name string
	Kind NodeKind

	Task     TaskFunc[T]
	Choice   ChoiceFunc[T]
	Decision *DecisionSpec[T]
	Branches []Branch[T]
	Delay    time.Duration
	Agent    *AgentSpec[T]

	// Next is the destination for Task, Parallel, Wait and Agent nodes.
	NextNode string
	// Routes maps route keys to destinations for Choice and Decision nodes.
	Routes map[string]string
	// RouteKeys preserves declaration order for deterministic rendering.
	RouteKeys []string

	Retry       *RetryPolicy
	ErrorRoutes []ErrorRoute
	FailMessage string

	// ParallelCopy, when non-nil, creates an isolated copy of state for each
	// parallel branch. Branches mutate their own copy, so no mutex is needed
	// for disjoint writes. ParallelMerge is required when Copy is set.
	ParallelCopy  func(*T) *T
	ParallelMerge func(main *T, copies []*T) error

	// SubWorkflow, when non-nil, runs a nested workflow as a single node.
	// The sub-workflow shares the parent's state (*T). On completion, the
	// parent continues at NextNode. On failure, a SubWorkflowError is
	// returned with the sub-workflow's trace attached.
	SubWorkflow *Workflow[T]

	workflow *Workflow[T]
}

// Workflow is a static, typed workflow graph.
type Workflow[T any] struct {
	// Name is used in results and diagram headers.
	Name string

	nodes map[string]*Node[T]
	order []string
	start string
}

// New creates an empty workflow. The first node added becomes the start node
// unless StartAt overrides it.
func New[T any](name string) *Workflow[T] {
	return &Workflow[T]{
		Name:  name,
		nodes: make(map[string]*Node[T]),
	}
}

// StartAt sets the node where execution begins.
func (w *Workflow[T]) StartAt(name string) *Workflow[T] {
	w.start = name
	return w
}

// Start returns the configured start node name.
func (w *Workflow[T]) Start() string {
	return w.start
}

// Nodes returns node names in declaration order.
func (w *Workflow[T]) Nodes() []string {
	out := make([]string, len(w.order))
	copy(out, w.order)
	return out
}

// Node returns a node by name, or nil when it does not exist.
func (w *Workflow[T]) Node(name string) *Node[T] {
	return w.nodes[name]
}

func (w *Workflow[T]) add(n *Node[T]) *Node[T] {
	if n.Name == "" {
		panic("flow: node name must not be empty")
	}
	if _, exists := w.nodes[n.Name]; exists {
		panic(fmt.Sprintf("flow: duplicate node %q", n.Name))
	}
	n.workflow = w
	n.Routes = make(map[string]string)
	w.nodes[n.Name] = n
	w.order = append(w.order, n.Name)
	if w.start == "" {
		w.start = n.Name
	}
	return n
}

// Task adds a Task node.
func (w *Workflow[T]) Task(name string, fn TaskFunc[T]) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindTask, Task: fn})
}

// Choice adds a deterministic Choice node. Declare destinations with When and
// Otherwise.
func (w *Workflow[T]) Choice(name string, fn ChoiceFunc[T]) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindChoice, Choice: fn})
}

// Decision adds a Decision node whose route is selected by a policy.
func (w *Workflow[T]) Decision(name string, spec DecisionSpec[T]) *Node[T] {
	n := &Node[T]{Name: name, Kind: KindDecision, Decision: &spec}
	w.add(n)
	for _, opt := range spec.Options {
		n.route(opt.Key, opt.Target)
	}
	return n
}

// Parallel adds a Parallel node.
func (w *Workflow[T]) Parallel(name string, branches ...Branch[T]) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindParallel, Branches: branches})
}

// Wait adds a Wait node.
func (w *Workflow[T]) Wait(name string, delay time.Duration) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindWait, Delay: delay})
}

// ParallelIsolated adds a Parallel node where each branch receives its own
// copy of the workflow state. Branches cannot race on shared fields because
// they mutate isolated copies. After all branches complete, merge combines
// the copies back into the main state.
//
// Use this when branches write to overlapping state fields and you want to
// avoid mutexes. Copy must return a deep copy of state; Merge receives the
// main state and all branch copies and should fold the results back in.
func (w *Workflow[T]) ParallelIsolated(name string, copy func(*T) *T, merge func(main *T, copies []*T) error, branches ...Branch[T]) *Node[T] {
	n := &Node[T]{
		Name:          name,
		Kind:          KindParallel,
		Branches:      branches,
		ParallelCopy:  copy,
		ParallelMerge: merge,
	}
	return w.add(n)
}

// Agent adds an Agent node.
func (w *Workflow[T]) Agent(name string, spec AgentSpec[T]) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindAgent, Agent: &spec})
}

// SubWorkflow adds a nested workflow as a single node. The sub-workflow runs
// synchronously and shares the parent's state. On completion, the parent
// continues at NextNode. On failure, a SubWorkflowError is returned with the
// sub-workflow's trace attached. Sub-workflows can be nested to any depth.
func (w *Workflow[T]) SubWorkflow(name string, sub *Workflow[T]) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindSubWorkflow, SubWorkflow: sub})
}

// Succeed adds a terminal successful node.
func (w *Workflow[T]) Succeed(name string) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindSucceed})
}

// Fail adds a terminal failing node.
func (w *Workflow[T]) Fail(name, message string) *Node[T] {
	return w.add(&Node[T]{Name: name, Kind: KindFail, FailMessage: message})
}

// Next sets the destination for Task, Parallel, Wait and Agent nodes.
func (n *Node[T]) Next(target string) *Node[T] {
	n.NextNode = target
	return n
}

// When declares a Choice or Decision route. It panics on duplicate keys so
// definition mistakes surface while the graph is built.
func (n *Node[T]) When(key, target string) *Node[T] {
	if key == "" {
		panic(fmt.Sprintf("flow: node %q: route key must not be empty", n.Name))
	}
	n.route(key, target)
	return n
}

// Otherwise declares the fallback Choice route.
func (n *Node[T]) Otherwise(target string) *Node[T] {
	n.route(DefaultRoute, target)
	return n
}

func (n *Node[T]) route(key, target string) {
	if key == "" {
		panic(fmt.Sprintf("flow: node %q: route key must not be empty", n.Name))
	}
	if target == "" {
		panic(fmt.Sprintf("flow: node %q: route %q has no target", n.Name, key))
	}
	if n.Routes == nil {
		n.Routes = make(map[string]string)
	}
	if _, exists := n.Routes[key]; exists {
		panic(fmt.Sprintf("flow: node %q: duplicate route %q", n.Name, key))
	}
	n.Routes[key] = target
	n.RouteKeys = append(n.RouteKeys, key)
}

// WithRetry attaches a retry policy to a Task node.
func (n *Node[T]) WithRetry(policy RetryPolicy) *Node[T] {
	p := policy
	n.Retry = &p
	return n
}

// Catch routes matching errors to target instead of failing the workflow.
// Routes are checked in declaration order; a nil match accepts any error.
func (n *Node[T]) Catch(target string, match func(error) bool) *Node[T] {
	n.ErrorRoutes = append(n.ErrorRoutes, ErrorRoute{Target: target, Match: match})
	return n
}

// Validate checks the graph before it runs. It verifies that functions, routes
// and agent tools are present and that every destination exists.
func (w *Workflow[T]) Validate() error {
	if w == nil {
		return fmt.Errorf("flow: nil workflow")
	}
	if len(w.nodes) == 0 {
		return fmt.Errorf("flow: workflow %q has no nodes", w.Name)
	}
	if w.start == "" {
		return fmt.Errorf("flow: workflow %q has no start node", w.Name)
	}
	if _, ok := w.nodes[w.start]; !ok {
		return fmt.Errorf("flow: workflow %q: start node %q does not exist", w.Name, w.start)
	}
	for _, name := range w.order {
		if err := w.nodes[name].validate(); err != nil {
			return fmt.Errorf("flow: workflow %q: %w", w.Name, err)
		}
	}
	return nil
}

func (n *Node[T]) validate() error {
	switch n.Kind {
	case KindTask:
		if n.Task == nil {
			return fmt.Errorf("node %q: task function is nil", n.Name)
		}
	case KindChoice:
		if n.Choice == nil {
			return fmt.Errorf("node %q: choice function is nil", n.Name)
		}
		if len(n.Routes) == 0 {
			return fmt.Errorf("node %q: choice has no routes", n.Name)
		}
	case KindDecision:
		if n.Decision == nil {
			return fmt.Errorf("node %q: decision spec is nil", n.Name)
		}
		if n.Decision.Choose == nil {
			return fmt.Errorf("node %q: decision policy is nil", n.Name)
		}
		if len(n.Decision.Options) == 0 {
			return fmt.Errorf("node %q: decision has no options", n.Name)
		}
		seen := make(map[string]bool, len(n.Decision.Options))
		for _, opt := range n.Decision.Options {
			if opt.Key == "" {
				return fmt.Errorf("node %q: decision option has no key", n.Name)
			}
			if opt.Target == "" {
				return fmt.Errorf("node %q: decision option %q has no target", n.Name, opt.Key)
			}
			if seen[opt.Key] {
				return fmt.Errorf("node %q: duplicate decision option %q", n.Name, opt.Key)
			}
			seen[opt.Key] = true
		}
		if len(n.RouteKeys) != len(n.Decision.Options) {
			return fmt.Errorf("node %q: decision routes must be declared through Options", n.Name)
		}
		for i, opt := range n.Decision.Options {
			if n.RouteKeys[i] != opt.Key || n.Routes[opt.Key] != opt.Target {
				return fmt.Errorf("node %q: decision route %q does not match its option", n.Name, opt.Key)
			}
		}
	case KindParallel:
		if len(n.Branches) == 0 {
			return fmt.Errorf("node %q: parallel has no branches", n.Name)
		}
		seen := make(map[string]bool, len(n.Branches))
		for _, b := range n.Branches {
			if b.Name == "" {
				return fmt.Errorf("node %q: parallel branch has no name", n.Name)
			}
			if b.Run == nil {
				return fmt.Errorf("node %q: branch %q has no function", n.Name, b.Name)
			}
			if seen[b.Name] {
				return fmt.Errorf("node %q: duplicate branch %q", n.Name, b.Name)
			}
			seen[b.Name] = true
		}
		if n.ParallelCopy != nil && n.ParallelMerge == nil {
			return fmt.Errorf("node %q: parallel copy requires a merge function", n.Name)
		}
	case KindWait:
		if n.Delay < 0 {
			return fmt.Errorf("node %q: wait duration must not be negative", n.Name)
		}
	case KindAgent:
		if n.Agent == nil {
			return fmt.Errorf("node %q: agent spec is nil", n.Name)
		}
		if n.Agent.Policy == nil {
			return fmt.Errorf("node %q: agent policy is nil", n.Name)
		}
		if n.Agent.MaxIterations < 0 {
			return fmt.Errorf("node %q: agent max iterations must not be negative", n.Name)
		}
		seen := make(map[string]bool, len(n.Agent.Tools))
		for _, t := range n.Agent.Tools {
			if t.Name == "" {
				return fmt.Errorf("node %q: agent tool has no name", n.Name)
			}
			if t.Run == nil {
				return fmt.Errorf("node %q: agent tool %q has no function", n.Name, t.Name)
			}
			if seen[t.Name] {
				return fmt.Errorf("node %q: duplicate agent tool %q", n.Name, t.Name)
			}
			seen[t.Name] = true
		}
	case KindSubWorkflow:
		if n.SubWorkflow == nil {
			return fmt.Errorf("node %q: sub-workflow is nil", n.Name)
		}
		if err := n.SubWorkflow.Validate(); err != nil {
			return fmt.Errorf("node %q: sub-workflow validation: %w", n.Name, err)
		}
	case KindSucceed, KindFail:
	default:
		return fmt.Errorf("node %q: unknown kind %q", n.Name, n.Kind)
	}

	if n.Retry != nil {
		if n.Kind != KindTask {
			return fmt.Errorf("node %q: retry is only supported on Task nodes", n.Name)
		}
		if n.Retry.MaxAttempts < 1 {
			return fmt.Errorf("node %q: retry MaxAttempts must be at least 1", n.Name)
		}
		if n.Retry.InitialInterval < 0 {
			return fmt.Errorf("node %q: retry InitialInterval must not be negative", n.Name)
		}
		if n.Retry.Backoff != 0 && n.Retry.Backoff < 1 {
			return fmt.Errorf("node %q: retry Backoff must be at least 1", n.Name)
		}
		if n.Retry.MaxInterval < 0 {
			return fmt.Errorf("node %q: retry MaxInterval must not be negative", n.Name)
		}
	}

	for _, route := range n.RouteKeys {
		if err := n.checkTarget(n.Routes[route]); err != nil {
			return fmt.Errorf("node %q: route %q: %w", n.Name, route, err)
		}
	}
	for _, c := range n.ErrorRoutes {
		if c.Target == "" {
			return fmt.Errorf("node %q: error route has no target", n.Name)
		}
		if err := n.checkTarget(c.Target); err != nil {
			return fmt.Errorf("node %q: error route: %w", n.Name, err)
		}
	}

	switch n.Kind {
	case KindSucceed, KindFail:
		if n.NextNode != "" {
			return fmt.Errorf("node %q: terminal node must not have Next", n.Name)
		}
		if len(n.Routes) > 0 {
			return fmt.Errorf("node %q: terminal node must not have routes", n.Name)
		}
		if len(n.ErrorRoutes) > 0 {
			return fmt.Errorf("node %q: terminal node must not have error routes", n.Name)
		}
	case KindChoice, KindDecision:
		if n.NextNode != "" {
			return fmt.Errorf("node %q: use routes instead of Next", n.Name)
		}
	default:
		if n.NextNode == "" {
			return fmt.Errorf("node %q: Next is not set", n.Name)
		}
		if len(n.Routes) > 0 {
			return fmt.Errorf("node %q: routes are only valid on Choice and Decision", n.Name)
		}
	}
	if n.NextNode != "" {
		if err := n.checkTarget(n.NextNode); err != nil {
			return fmt.Errorf("node %q: Next: %w", n.Name, err)
		}
	}
	return nil
}

func (n *Node[T]) checkTarget(target string) error {
	if target == "" {
		return fmt.Errorf("target must not be empty")
	}
	if n.workflow == nil {
		return fmt.Errorf("node is not attached to a workflow")
	}
	if _, ok := n.workflow.nodes[target]; !ok {
		return fmt.Errorf("target node %q does not exist", target)
	}
	return nil
}
