package main

import (
	"context"
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
