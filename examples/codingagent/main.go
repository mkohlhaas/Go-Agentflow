// Command codingagent is a runnable demonstration of the agentflow package.
//
// It builds a small Go module in a temporary directory, asks a deterministic
// mock LLM to repair a buggy function, and uses real tool calls to read the
// file, apply a patch and run the test suite. The workflow graph is fixed; the
// policies only choose among declared routes and tools.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"agentflow/flow"
)

// State is the single typed state shared by every node in the workflow.
type State struct {
	Root         string
	Goal         string
	TestOutput   string
	TestsPassed  bool
	PatchApplied bool
	Log          []string
}

func main() {
	mermaidOnly := flag.Bool("mermaid", false, "print the workflow diagram and exit")
	keep := flag.Bool("keep", false, "keep the temporary workspace")
	trace := flag.Bool("trace", true, "print the execution trace")
	flag.Parse()

	wf := buildWorkflow()
	if *mermaidOnly {
		fmt.Print(wf.Mermaid())
		return
	}

	var state State
	defer func() {
		if state.Root == "" {
			return
		}
		if *keep {
			fmt.Printf("workspace kept at %s\n", state.Root)
			return
		}
		_ = os.RemoveAll(state.Root)
	}()

	res, err := wf.Run(context.Background(), &state)
	if *trace && res != nil {
		printTrace(res)
	}

	fmt.Printf("\ngoal: %s\n", state.Goal)
	fmt.Printf("workspace: %s\n", state.Root)
	fmt.Printf("patch applied: %v\n", state.PatchApplied)
	fmt.Printf("tests passed: %v\n", state.TestsPassed)

	if err != nil {
		fmt.Fprintf(os.Stderr, "\nworkflow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nworkflow %s completed at node %s in %d steps\n", res.Status, res.LastNode, res.Steps)
}

func buildWorkflow() *flow.Workflow[State] {
	wf := flow.New[State]("coding-agent")

	wf.Task("prepare", prepareWorkspace).Next("triage")
	wf.Decision("triage", flow.DecisionSpec[State]{
		Options: []flow.RouteOption{
			{Key: "repair", Description: "Let the coding agent inspect and patch the failing code.", Target: "repair"},
			{Key: "stop", Description: "Abort instead of attempting a repair.", Target: "escalate"},
		},
		Choose:      triagePolicy{},
		MaxAttempts: 2,
	})
	wf.Agent("repair", flow.AgentSpec[State]{
		Goal:          "Read the failing implementation, patch it, and verify with go test.",
		Policy:        repairPolicy{},
		Tools:         codingTools(),
		MaxIterations: 6,
	}).Next("gate")
	wf.Choice("gate", func(_ context.Context, s *State) (string, error) {
		if s.TestsPassed {
			return "pass", nil
		}
		return "", nil
	}).When("pass", "report").Otherwise("escalate")
	wf.Succeed("report")
	wf.Fail("escalate", "automatic repair did not make the tests pass")

	return wf
}

// triagePolicy is the mock LLM for the Decision node. It sees only the declared
// options, so an invalid route is impossible without the engine rejecting it.
type triagePolicy struct{}

func (triagePolicy) Choose(_ context.Context, s *State, options []flow.RouteOption) (string, error) {
	for _, option := range options {
		if option.Key == "repair" && s.Root != "" {
			return option.Key, nil
		}
	}
	return "stop", nil
}

// repairPolicy is the mock LLM for the Agent node. It performs a fixed sequence
// of tool calls but reacts to tool errors and to the test result, which is the
// same feedback loop a real LLM would use.
type repairPolicy struct{}

func (repairPolicy) Next(_ context.Context, s *State, history []flow.AgentTurn) (flow.AgentDecision, error) {
	if len(history) > 0 {
		last := history[len(history)-1]
		if last.Error != "" && last.Tool == "apply_patch" {
			return flow.AgentDecision{Fail: true, Message: "patch failed: " + last.Error}, nil
		}
	}

	switch len(history) {
	case 0:
		return flow.AgentDecision{
			Thought: "Read the implementation before changing it.",
			Tool:    "read_file",
			Args:    map[string]string{"path": "math.go"},
		}, nil
	case 1:
		return flow.AgentDecision{
			Thought: "Add subtracts instead of adds. Replace the body with a + b.",
			Tool:    "apply_patch",
			Args: map[string]string{
				"path": "math.go",
				"old":  "return a - b",
				"new":  "return a + b",
			},
		}, nil
	case 2:
		return flow.AgentDecision{
			Thought: "Patch applied. Let the compiler and tests decide whether it is correct.",
			Tool:    "run_tests",
		}, nil
	default:
		if !s.TestsPassed {
			return flow.AgentDecision{
				Fail:    true,
				Message: "tests are still failing: " + s.TestOutput,
			}, nil
		}
		return flow.AgentDecision{Done: true, Message: "go test passes"}, nil
	}
}

func codingTools() []flow.Tool[State] {
	return []flow.Tool[State]{
		{
			Name:        "read_file",
			Description: "Read a UTF-8 file relative to the workspace root.",
			Run:         readFile,
		},
		{
			Name:        "apply_patch",
			Description: "Replace the first occurrence of old with new in a workspace file.",
			Run:         applyPatch,
		},
		{
			Name:        "run_tests",
			Description: "Run go test ./... in the workspace and record the result.",
			Run:         runTests,
		},
	}
}

func prepareWorkspace(ctx context.Context, s *State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.MkdirTemp("", "agentflow-coding-")
	if err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}

	files := map[string]string{
		"go.mod": "module example.com/codingagent\n\ngo 1.24\n",
		"math.go": `package calc

func Add(a, b int) int {
	return a - b
}
`,
		"math_test.go": `package calc

import "testing"

func TestAdd(t *testing.T) {
	if got := Add(2, 3); got != 5 {
		t.Fatalf("Add(2, 3) = %d, want 5", got)
	}
}
`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			_ = os.RemoveAll(root)
			return fmt.Errorf("write %s: %w", name, err)
		}
	}

	s.Root = root
	s.Goal = "Fix Add so that the existing Go test passes."
	s.Log = append(s.Log, "created workspace "+root)
	return nil
}

func readFile(_ context.Context, s *State, args map[string]string) (string, error) {
	path, err := workspacePath(s.Root, args["path"])
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s.Log = append(s.Log, "read "+args["path"])
	return string(data), nil
}

func applyPatch(_ context.Context, s *State, args map[string]string) (string, error) {
	path, err := workspacePath(s.Root, args["path"])
	if err != nil {
		return "", err
	}
	oldText := args["old"]
	if oldText == "" {
		return "", errors.New("apply_patch: old text is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	content := string(data)
	if !strings.Contains(content, oldText) {
		return "", fmt.Errorf("apply_patch: %s does not contain the requested text", args["path"])
	}
	updated := strings.Replace(content, oldText, args["new"], 1)
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		return "", err
	}
	s.PatchApplied = true
	s.Log = append(s.Log, "patched "+args["path"])
	return "patched " + args["path"], nil
}

func runTests(ctx context.Context, s *State, _ map[string]string) (string, error) {
	cmd := exec.CommandContext(ctx, "go", "test", "./...")
	cmd.Dir = s.Root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	s.TestOutput = strings.TrimSpace(string(out))
	s.TestsPassed = err == nil
	s.Log = append(s.Log, fmt.Sprintf("go test passed=%v", s.TestsPassed))
	if err != nil {
		return s.TestOutput, fmt.Errorf("go test failed: %w", err)
	}
	return s.TestOutput, nil
}

func workspacePath(root, name string) (string, error) {
	if root == "" {
		return "", errors.New("workspace is not prepared")
	}
	if name == "" {
		return "", errors.New("path is required")
	}
	full := filepath.Join(root, filepath.Clean(name))
	rel, err := filepath.Rel(root, full)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes the workspace", name)
	}
	return full, nil
}

func printTrace(res *flow.Result[State]) {
	fmt.Println("trace:")
	for _, event := range res.Trace {
		parts := []string{
			fmt.Sprintf("%2d", event.Seq),
			fmt.Sprintf("%-18s", event.Kind),
			event.Node,
		}
		if event.Branch != "" {
			parts = append(parts, "branch="+event.Branch)
		}
		if event.Route != "" {
			parts = append(parts, "route="+event.Route)
		}
		if event.Tool != "" {
			parts = append(parts, "tool="+event.Tool)
		}
		if event.Message != "" {
			parts = append(parts, shorten(event.Message, 120))
		}
		fmt.Println("  " + strings.Join(parts, "  "))
	}
}

func shorten(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
