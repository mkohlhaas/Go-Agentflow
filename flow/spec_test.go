package flow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"agentflow/flow"
)

type specTestState struct {
	Value int
}

func specTestTask(context.Context, *specTestState) error { return nil }

func specTestChoice(context.Context, *specTestState) (string, error) { return "go", nil }

func specTestBranch(context.Context, *specTestState) error { return nil }

func specTestCopy(s *specTestState) *specTestState {
	copy := *s
	return &copy
}

func specTestMerge(*specTestState, []*specTestState) error { return nil }

func specTestTool(context.Context, *specTestState, map[string]string) (string, error) {
	return "", nil
}

type specTestMethod struct{}

func (specTestMethod) Run(context.Context, *specTestState) error { return nil }

type specTestPolicy struct{}

func (specTestPolicy) Choose(context.Context, *specTestState, []flow.RouteOption) (string, error) {
	return "parallel", nil
}

type specTestAgentPolicy struct{}

func (specTestAgentPolicy) Next(context.Context, *specTestState, []flow.AgentTurn) (flow.AgentDecision, error) {
	return flow.AgentDecision{Done: true}, nil
}

func buildSpecTestWorkflow() *flow.Workflow[specTestState] {
	wf := flow.New[specTestState]("all-kinds")
	wf.Task("task", specTestTask).
		WithRetry(flow.RetryPolicy{
			MaxAttempts:     3,
			InitialInterval: 10 * time.Millisecond,
			Backoff:         2,
			MaxInterval:     time.Second,
		}).
		Next("choice")

	wf.Choice("choice", specTestChoice).
		When("go", "decision").
		Otherwise("fail")

	wf.Decision("decision", flow.DecisionSpec[specTestState]{
		Options: []flow.RouteOption{
			{Key: "parallel", Description: "fan out", Target: "parallel"},
			{Key: "stop", Description: "stop", Target: "fail"},
		},
		Choose:      specTestPolicy{},
		MaxAttempts: 2,
	}).Catch("fail", nil)

	wf.Parallel("parallel", flow.Branch[specTestState]{
		Name: "one",
		Run:  specTestBranch,
	}).Next("isolated")

	wf.ParallelIsolated("isolated", specTestCopy, specTestMerge, flow.Branch[specTestState]{
		Name: "two",
		Run:  specTestBranch,
	}).Next("wait")

	wf.Wait("wait", time.Second).Next("agent")

	wf.Agent("agent", flow.AgentSpec[specTestState]{
		Goal:   "do the thing",
		Policy: specTestAgentPolicy{},
		Tools: []flow.Tool[specTestState]{
			{Name: "tool", Description: "desc", Run: specTestTool},
		},
		MaxIterations: 3,
	}).Next("sub")

	sub := flow.New[specTestState]("sub")
	sub.Task("sub-task", specTestTask).Next("sub-done")
	sub.Succeed("sub-done")
	wf.SubWorkflow("sub", sub).Next("done")

	wf.Succeed("done")
	wf.Fail("fail", "stop")

	return wf
}

func nodeSpecByName(t *testing.T, spec flow.Spec, name flow.NodeName) flow.NodeSpec {
	t.Helper()
	for _, node := range spec.Nodes {
		if node.Name == name {
			return node
		}
	}
	t.Fatalf("node %q not found in spec", name)
	return flow.NodeSpec{}
}

func TestSpecCoversAllNodeKinds(t *testing.T) {
	wf := buildSpecTestWorkflow()
	spec, err := wf.Spec()
	if err != nil {
		t.Fatalf("Spec returned error: %v", err)
	}

	if spec.SpecVersion != flow.SpecFormatVersion {
		t.Fatalf("SpecVersion = %d, want %d", spec.SpecVersion, flow.SpecFormatVersion)
	}
	if spec.Description != flow.SpecDescription {
		t.Fatalf("Description = %q, want %q", spec.Description, flow.SpecDescription)
	}
	if spec.Name != "all-kinds" {
		t.Fatalf("Name = %q, want all-kinds", spec.Name)
	}
	if !strings.Contains(spec.StateType, "specTestState") {
		t.Fatalf("StateType = %q, want it to mention specTestState", spec.StateType)
	}
	if spec.Start != "task" {
		t.Fatalf("Start = %q, want task", spec.Start)
	}
	if len(spec.Nodes) != 10 {
		t.Fatalf("len(Nodes) = %d, want 10", len(spec.Nodes))
	}
	if spec.Nodes[0].Name != "task" || spec.Nodes[len(spec.Nodes)-1].Name != "fail" {
		t.Fatalf("node order = %v, want declaration order", nodeNames(spec.Nodes))
	}

	task := nodeSpecByName(t, spec, "task")
	if task.Kind != flow.KindTask || task.Next != "choice" {
		t.Fatalf("task = %+v, want kind=task next=choice", task)
	}
	if task.Run == nil || !strings.HasSuffix(task.Run.Name, "specTestTask") || task.Run.Kind != flow.CodeRefKindFunc {
		t.Fatalf("task.Run = %+v, want a stable func ref for specTestTask", task.Run)
	}
	if task.Retry == nil {
		t.Fatal("task.Retry is nil")
	}
	if task.Retry.MaxAttempts != 3 || task.Retry.InitialInterval != "10ms" ||
		task.Retry.Backoff != 2 || task.Retry.MaxInterval != "1s" {
		t.Fatalf("task.Retry = %+v, want the declared retry policy", task.Retry)
	}

	choice := nodeSpecByName(t, spec, "choice")
	if choice.Kind != flow.KindChoice || choice.Choose == nil {
		t.Fatalf("choice = %+v, want a choice with a code ref", choice)
	}
	if !strings.HasSuffix(choice.Choose.Name, "specTestChoice") {
		t.Fatalf("choice.Choose = %+v, want specTestChoice", choice.Choose)
	}
	if len(choice.Routes) != 2 || choice.Routes[0].Key != "go" || choice.Routes[1].Key != "default" {
		t.Fatalf("choice.Routes = %+v, want [go default] in declaration order", choice.Routes)
	}

	decision := nodeSpecByName(t, spec, "decision")
	if decision.Kind != flow.KindDecision || decision.Choose == nil {
		t.Fatalf("decision = %+v, want a decision with a policy ref", decision)
	}
	if decision.Choose.Kind != flow.CodeRefKindType || !strings.HasSuffix(decision.Choose.Name, "specTestPolicy") {
		t.Fatalf("decision.Choose = %+v, want a type ref for specTestPolicy", decision.Choose)
	}
	if decision.MaxAttempts != 2 {
		t.Fatalf("decision.MaxAttempts = %d, want 2", decision.MaxAttempts)
	}
	if len(decision.Options) != 2 || decision.Options[0].Key != "parallel" ||
		decision.Options[0].Target != "parallel" || decision.Options[0].Description != "fan out" {
		t.Fatalf("decision.Options = %+v, want the declared options in order", decision.Options)
	}
	if len(decision.ErrorRoutes) != 1 || !decision.ErrorRoutes[0].Any ||
		decision.ErrorRoutes[0].Target != "fail" || decision.ErrorRoutes[0].Match != nil {
		t.Fatalf("decision.ErrorRoutes = %+v, want one catch-all route to fail", decision.ErrorRoutes)
	}

	parallel := nodeSpecByName(t, spec, "parallel")
	if parallel.Kind != flow.KindParallel || parallel.Next != "isolated" {
		t.Fatalf("parallel = %+v, want kind=parallel next=isolated", parallel)
	}
	if len(parallel.Branches) != 1 || parallel.Branches[0].Name != "one" || parallel.Branches[0].Run == nil {
		t.Fatalf("parallel.Branches = %+v, want branch one with a code ref", parallel.Branches)
	}
	if parallel.Copy != nil || parallel.Merge != nil {
		t.Fatalf("parallel copy/merge = %+v/%+v, want nil for a shared-state parallel", parallel.Copy, parallel.Merge)
	}

	isolated := nodeSpecByName(t, spec, "isolated")
	if isolated.Kind != flow.KindParallel || isolated.Next != "wait" {
		t.Fatalf("isolated = %+v, want kind=parallel next=wait", isolated)
	}
	if isolated.Copy == nil || !strings.HasSuffix(isolated.Copy.Name, "specTestCopy") {
		t.Fatalf("isolated.Copy = %+v, want specTestCopy", isolated.Copy)
	}
	if isolated.Merge == nil || !strings.HasSuffix(isolated.Merge.Name, "specTestMerge") {
		t.Fatalf("isolated.Merge = %+v, want specTestMerge", isolated.Merge)
	}

	wait := nodeSpecByName(t, spec, "wait")
	if wait.Kind != flow.KindWait || wait.Delay != "1s" {
		t.Fatalf("wait = %+v, want kind=wait delay=1s", wait)
	}

	agent := nodeSpecByName(t, spec, "agent")
	if agent.Kind != flow.KindAgent || agent.Goal != "do the thing" || agent.MaxIterations != 3 {
		t.Fatalf("agent = %+v, want the declared goal and iteration limit", agent)
	}
	if agent.Policy == nil || agent.Policy.Kind != flow.CodeRefKindType ||
		!strings.HasSuffix(agent.Policy.Name, "specTestAgentPolicy") {
		t.Fatalf("agent.Policy = %+v, want a type ref for specTestAgentPolicy", agent.Policy)
	}
	if len(agent.Tools) != 1 || agent.Tools[0].Name != "tool" ||
		agent.Tools[0].Description != "desc" || agent.Tools[0].Run == nil {
		t.Fatalf("agent.Tools = %+v, want tool with a code ref", agent.Tools)
	}

	sub := nodeSpecByName(t, spec, "sub")
	if sub.Kind != flow.KindSubWorkflow || sub.SubWorkflow == nil {
		t.Fatalf("sub = %+v, want an inlined sub-workflow", sub)
	}
	if sub.SubWorkflow.Name != "sub" || len(sub.SubWorkflow.Nodes) != 2 {
		t.Fatalf("sub.SubWorkflow = %+v, want the shared sub-workflow inline", sub.SubWorkflow)
	}

	done := nodeSpecByName(t, spec, "done")
	if done.Kind != flow.KindSucceed || done.Next != "" {
		t.Fatalf("done = %+v, want a terminal succeed node", done)
	}

	fail := nodeSpecByName(t, spec, "fail")
	if fail.Kind != flow.KindFail || fail.FailMessage != "stop" {
		t.Fatalf("fail = %+v, want kind=fail message=stop", fail)
	}
}

func nodeNames(nodes []flow.NodeSpec) []flow.NodeName {
	names := make([]flow.NodeName, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}
	return names
}

func TestSpecOmitsAnonymousFunctionRefs(t *testing.T) {
	wf := flow.New[specTestState]("anon")
	wf.Task("task", func(context.Context, *specTestState) error {
		return nil
	}).Next("choice")
	wf.Choice("choice", func(context.Context, *specTestState) (string, error) {
		return "done", nil
	}).When("done", "done").Otherwise("done")
	wf.Succeed("done")

	spec, err := wf.Spec()
	if err != nil {
		t.Fatalf("Spec returned error: %v", err)
	}
	if spec.Nodes[0].Run != nil {
		t.Fatalf("task.Run = %+v, want nil for an anonymous function", spec.Nodes[0].Run)
	}
	if spec.Nodes[1].Choose != nil {
		t.Fatalf("choice.Choose = %+v, want nil for an anonymous function", spec.Nodes[1].Choose)
	}
}

func TestSpecStripsMethodValueSuffix(t *testing.T) {
	method := specTestMethod{}
	wf := flow.New[specTestState]("method")
	wf.Task("task", method.Run).Next("done")
	wf.Succeed("done")

	spec, err := wf.Spec()
	if err != nil {
		t.Fatalf("Spec returned error: %v", err)
	}
	run := spec.Nodes[0].Run
	if run == nil {
		t.Fatal("task.Run is nil, want a method ref")
	}
	if strings.HasSuffix(run.Name, "-fm") {
		t.Fatalf("task.Run.Name = %q, want the -fm suffix removed", run.Name)
	}
	if !strings.Contains(run.Name, "specTestMethod.Run") {
		t.Fatalf("task.Run.Name = %q, want it to name specTestMethod.Run", run.Name)
	}
}

func TestMarshalSpecDeterministic(t *testing.T) {
	wf := buildSpecTestWorkflow()

	first, err := wf.MarshalSpec()
	if err != nil {
		t.Fatalf("MarshalSpec returned error: %v", err)
	}
	second, err := wf.MarshalSpec()
	if err != nil {
		t.Fatalf("MarshalSpec returned error on second call: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("MarshalSpec output is not deterministic")
	}
	if len(first) == 0 || first[len(first)-1] != '\n' {
		t.Fatal("MarshalSpec output should end with a newline")
	}
	if !json.Valid(first) {
		t.Fatalf("MarshalSpec output is not valid JSON:\n%s", first)
	}

	var decoded flow.Spec
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatalf("unmarshal spec: %v", err)
	}
	if len(decoded.Nodes) != len(wf.Nodes()) {
		t.Fatalf("decoded nodes = %d, want %d", len(decoded.Nodes), len(wf.Nodes()))
	}
}

func TestSpecRejectsSubWorkflowCycle(t *testing.T) {
	wf := flow.New[specTestState]("self")
	wf.SubWorkflow("loop", wf).Next("done")
	wf.Succeed("done")

	if _, err := wf.Spec(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Spec error = %v, want a cycle error", err)
	}
	if _, err := wf.MarshalSpec(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("MarshalSpec error = %v, want a cycle error", err)
	}
}

func TestSpecInlinesSharedSubWorkflow(t *testing.T) {
	sub := flow.New[specTestState]("shared")
	sub.Task("work", specTestTask).Next("sub-done")
	sub.Succeed("sub-done")

	wf := flow.New[specTestState]("parent")
	wf.SubWorkflow("one", sub).Next("two")
	wf.SubWorkflow("two", sub).Next("done")
	wf.Succeed("done")

	spec, err := wf.Spec()
	if err != nil {
		t.Fatalf("Spec returned error for a shared sub-workflow: %v", err)
	}
	if spec.Nodes[0].SubWorkflow == nil || spec.Nodes[1].SubWorkflow == nil {
		t.Fatal("both parent nodes should inline the shared sub-workflow")
	}
	if spec.Nodes[0].SubWorkflow.Name != "shared" || spec.Nodes[1].SubWorkflow.Name != "shared" {
		t.Fatalf("inlined sub-workflows = %q/%q, want shared/shared",
			spec.Nodes[0].SubWorkflow.Name, spec.Nodes[1].SubWorkflow.Name)
	}
}

func TestSpecNilWorkflow(t *testing.T) {
	var wf *flow.Workflow[specTestState]
	if _, err := wf.Spec(); err == nil {
		t.Fatal("Spec on a nil workflow should return an error")
	}
}
