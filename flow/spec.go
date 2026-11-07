package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

const (
	// SpecFormatVersion is the version of the serialized workflow description.
	SpecFormatVersion = 1
	// SpecDescription is included in every exported specification to make the
	// format's limited scope explicit.
	SpecDescription = "structure only; behavior is implemented in Go code"

	// CodeRefKindFunc marks a CodeRef that names a function or method.
	CodeRefKindFunc = "func"
	// CodeRefKindType marks a CodeRef that names a concrete policy type.
	CodeRefKindType = "type"
)

// Spec is a serializable description of a workflow's static structure. It
// describes nodes, routes and configuration, not the behavior of Go functions
// or policies. Anonymous closures are intentionally omitted from CodeRefs
// because their runtime names are not stable across edits.
type Spec struct {
	SpecVersion int        `json:"specVersion"`
	Description string     `json:"description,omitempty"`
	Name        string     `json:"name"`
	StateType   string     `json:"stateType,omitempty"`
	Start       NodeName   `json:"start"`
	Nodes       []NodeSpec `json:"nodes"`
}

// NodeSpec describes one node in a Spec. Only the fields relevant to the
// node's Kind are populated.
type NodeSpec struct {
	Name NodeName `json:"name"`
	Kind NodeKind `json:"kind"`
	Next NodeName `json:"next,omitempty"`

	// Run is the Task implementation. Choose is the Choice function or the
	// Decision policy. Both are omitted for anonymous closures.
	Run    *CodeRef    `json:"run,omitempty"`
	Choose *CodeRef    `json:"choose,omitempty"`
	Routes []RouteSpec `json:"routes,omitempty"`

	// Decision configuration.
	Options     []OptionSpec `json:"options,omitempty"`
	MaxAttempts int          `json:"maxAttempts,omitempty"`

	// Parallel configuration.
	Branches []BranchSpec `json:"branches,omitempty"`
	Copy     *CodeRef     `json:"copy,omitempty"`
	Merge    *CodeRef     `json:"merge,omitempty"`

	// Wait configuration.
	Delay string `json:"delay,omitempty"`

	// ToolLoop configuration.
	Goal          string     `json:"goal,omitempty"`
	Policy        *CodeRef   `json:"policy,omitempty"`
	Tools         []ToolSpec `json:"tools,omitempty"`
	MaxIterations int        `json:"maxIterations,omitempty"`

	// SubWorkflow is the inlined description of a nested workflow.
	SubWorkflow *Spec `json:"subWorkflow,omitempty"`

	// Fail configuration.
	FailMessage string `json:"failMessage,omitempty"`

	Retry       *RetrySpec       `json:"retry,omitempty"`
	ErrorRoutes []ErrorRouteSpec `json:"errorRoutes,omitempty"`
}

// RouteSpec is one declared Choice route.
type RouteSpec struct {
	Key    string   `json:"key"`
	Target NodeName `json:"target"`
}

// OptionSpec is one declared Decision option, including its description.
type OptionSpec struct {
	Key         string   `json:"key"`
	Target      NodeName `json:"target"`
	Description string   `json:"description,omitempty"`
}

// BranchSpec is one arm of a Parallel node.
type BranchSpec struct {
	Name NodeName `json:"name"`
	Run  *CodeRef `json:"run,omitempty"`
}

// ToolSpec is one tool exposed to a ToolLoop policy.
type ToolSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Run         *CodeRef `json:"run,omitempty"`
}

// RetrySpec is the serialized form of a RetryPolicy.
type RetrySpec struct {
	MaxAttempts     int     `json:"maxAttempts"`
	InitialInterval string  `json:"initialInterval,omitempty"`
	Backoff         float64 `json:"backoff,omitempty"`
	MaxInterval     string  `json:"maxInterval,omitempty"`
}

// ErrorRouteSpec is one Catch route. Any is true when the route has a nil
// matcher and therefore catches every error. Match is omitted for anonymous
// matcher closures.
type ErrorRouteSpec struct {
	Target NodeName `json:"target"`
	Any    bool     `json:"any,omitempty"`
	Match  *CodeRef `json:"match,omitempty"`
}

// CodeRef identifies a Go implementation when it can be named stably. Kind is
// "func" for functions and methods, or "type" for a policy's concrete type.
type CodeRef struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// Spec returns a serializable description of the workflow's static structure.
// It does not validate the graph and does not describe function behavior.
// Sub-workflows are inlined; a sub-workflow cycle is reported as an error.
func (w *Workflow[T]) Spec() (Spec, error) {
	return w.buildSpec(make(map[*Workflow[T]]bool))
}

// MarshalSpec returns the workflow description as deterministic, indented JSON
// with a trailing newline. Anonymous closures are omitted from CodeRefs.
func (w *Workflow[T]) MarshalSpec() ([]byte, error) {
	spec, err := w.Spec()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(spec); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (w *Workflow[T]) buildSpec(visiting map[*Workflow[T]]bool) (Spec, error) {
	if w == nil {
		return Spec{}, fmt.Errorf("flow: nil workflow")
	}
	if visiting[w] {
		return Spec{}, fmt.Errorf("flow: workflow %q: sub-workflow cycle detected", w.Name)
	}
	visiting[w] = true
	defer delete(visiting, w)

	spec := Spec{
		SpecVersion: SpecFormatVersion,
		Description: SpecDescription,
		Name:        w.Name,
		StateType:   reflect.TypeFor[T]().String(),
		Start:       w.start,

		Nodes: make([]NodeSpec, 0, len(w.order)),
	}
	for _, name := range w.order {
		nodeSpec, err := buildNodeSpec(w.nodes[name], visiting)
		if err != nil {
			return Spec{}, fmt.Errorf("flow: workflow %q: %w", w.Name, err)
		}
		spec.Nodes = append(spec.Nodes, nodeSpec)
	}
	return spec, nil
}

func buildNodeSpec[T any](node *Node[T], visiting map[*Workflow[T]]bool) (NodeSpec, error) {
	spec := NodeSpec{
		Name: node.Name,
		Kind: node.Kind,
		Next: node.NextNode,
	}

	switch node.Kind {
	case KindTask:
		spec.Run = codeRefForValue(node.Task)

	case KindChoice:
		spec.Choose = codeRefForValue(node.Choice)
		for _, key := range node.RouteKeys {
			spec.Routes = append(spec.Routes, RouteSpec{
				Key:    key,
				Target: node.Routes[key],
			})
		}

	case KindDecision:
		if node.Decision != nil {
			spec.Choose = codeRefForValue(node.Decision.Choose)
			spec.MaxAttempts = node.Decision.MaxAttempts
			for _, option := range node.Decision.Options {
				spec.Options = append(spec.Options, OptionSpec{
					Key:         option.Key,
					Target:      option.Target,
					Description: option.Description,
				})
			}
		}

	case KindParallel:
		for _, branch := range node.Branches {
			spec.Branches = append(spec.Branches, BranchSpec{
				Name: branch.Name,
				Run:  codeRefForValue(branch.Run),
			})
		}
		spec.Copy = codeRefForValue(node.ParallelCopy)
		spec.Merge = codeRefForValue(node.ParallelMerge)

	case KindWait:
		spec.Delay = node.Delay.String()

	case KindToolLoop:
		if node.ToolLoop != nil {
			spec.Goal = node.ToolLoop.Goal
			spec.Policy = codeRefForValue(node.ToolLoop.Policy)
			spec.MaxIterations = node.ToolLoop.MaxIterations
			for _, tool := range node.ToolLoop.Tools {
				spec.Tools = append(spec.Tools, ToolSpec{
					Name:        tool.Name,
					Description: tool.Description,
					Run:         codeRefForValue(tool.Run),
				})
			}
		}

	case KindSubWorkflow:
		if node.SubWorkflow != nil {
			sub, err := node.SubWorkflow.buildSpec(visiting)
			if err != nil {
				return NodeSpec{}, fmt.Errorf("node %q: %w", node.Name, err)
			}
			spec.SubWorkflow = &sub
		}

	case KindFail:
		spec.FailMessage = node.FailMessage
	}

	if node.Retry != nil {
		retry := &RetrySpec{
			MaxAttempts: node.Retry.MaxAttempts,
			Backoff:     node.Retry.Backoff,
		}
		if node.Retry.InitialInterval != 0 {
			retry.InitialInterval = node.Retry.InitialInterval.String()
		}
		if node.Retry.MaxInterval != 0 {
			retry.MaxInterval = node.Retry.MaxInterval.String()
		}
		spec.Retry = retry
	}

	for _, route := range node.ErrorRoutes {
		errorRoute := ErrorRouteSpec{Target: route.Target}
		if route.Match == nil {
			errorRoute.Any = true
		} else {
			errorRoute.Match = codeRefForValue(route.Match)
		}
		spec.ErrorRoutes = append(spec.ErrorRoutes, errorRoute)
	}

	return spec, nil
}

// codeRefForValue returns a stable reference for a function, method or policy
// value. Anonymous functions are omitted because their runtime names include
// a compiler-assigned index that changes when the enclosing function changes.
func codeRefForValue(value any) *CodeRef {
	if value == nil {
		return nil
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil
	}
	if rv.Kind() == reflect.Func {
		if rv.IsNil() {
			return nil
		}
		fn := runtime.FuncForPC(rv.Pointer())
		if fn == nil {
			return nil
		}
		name := strings.TrimSuffix(fn.Name(), "-fm")
		if name == "" || isAnonymousFuncName(name) {
			return nil
		}
		return &CodeRef{Name: name, Kind: CodeRefKindFunc}
	}

	t := rv.Type()
	// Only named types make stable refs. Pointer types are named through their
	// element type; anonymous structs and interfaces are not useful as keys.
	base := t
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Name() == "" && base.Kind() != reflect.Interface {
		return nil
	}
	return &CodeRef{Name: t.String(), Kind: CodeRefKindType}
}

func isAnonymousFuncName(name string) bool {
	const marker = ".func"
	_, after, ok := strings.Cut(name, marker)
	if !ok {
		return false
	}
	rest := after
	return rest != "" && rest[0] >= '0' && rest[0] <= '9'
}
