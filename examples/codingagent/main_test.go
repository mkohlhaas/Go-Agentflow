package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"agentflow/flow"
)

func TestCodingAgentRepairsBug(t *testing.T) {
	wf := buildWorkflow()

	var state State
	res, err := wf.Run(context.Background(), &state)
	if state.Root != "" {
		t.Cleanup(func() { _ = os.RemoveAll(state.Root) })
	}
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Status != flow.StatusSucceeded {
		t.Fatalf("status = %q, want succeeded", res.Status)
	}
	if !state.PatchApplied {
		t.Fatal("agent did not apply a patch")
	}
	if !state.TestsPassed {
		t.Fatalf("tests did not pass: %s", state.TestOutput)
	}
	if !strings.Contains(state.TestOutput, "ok") {
		t.Fatalf("unexpected go test output: %q", state.TestOutput)
	}
	if !strings.Contains(wf.Mermaid(), "coding-agent") {
		t.Fatal("Mermaid output does not mention the workflow name")
	}
}

func TestCodingAgentSpec(t *testing.T) {
	wf := buildWorkflow()

	data, err := wf.MarshalSpec()
	if err != nil {
		t.Fatalf("MarshalSpec returned error: %v", err)
	}

	var spec flow.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("unmarshal spec: %v", err)
	}
	if spec.Name != "coding-agent" {
		t.Fatalf("spec name = %q, want coding-agent", spec.Name)
	}
	if spec.Start != "prepare" {
		t.Fatalf("spec start = %q, want prepare", spec.Start)
	}

	byName := make(map[flow.NodeName]flow.NodeSpec, len(spec.Nodes))
	for _, node := range spec.Nodes {
		byName[node.Name] = node
	}

	if ref := byName["prepare"].Run; ref == nil || !strings.HasSuffix(ref.Name, "prepareWorkspace") {
		t.Fatalf("prepare run ref = %+v, want prepareWorkspace", ref)
	}
	if ref := byName["triage"].Choose; ref == nil || ref.Kind != flow.CodeRefKindType ||
		!strings.HasSuffix(ref.Name, "triagePolicy") {
		t.Fatalf("triage choose ref = %+v, want triagePolicy type", ref)
	}
	if ref := byName["repair"].Policy; ref == nil || ref.Kind != flow.CodeRefKindType ||
		!strings.HasSuffix(ref.Name, "repairPolicy") {
		t.Fatalf("repair policy ref = %+v, want repairPolicy type", ref)
	}
	if got := len(byName["repair"].Tools); got != 3 {
		t.Fatalf("repair tools = %d, want 3", got)
	}
	// The gate choice is an anonymous closure, so it must be omitted rather
	// than exported under an unstable runtime name.
	if ref := byName["gate"].Choose; ref != nil {
		t.Fatalf("gate choose ref = %+v, want nil for an anonymous closure", ref)
	}
}
