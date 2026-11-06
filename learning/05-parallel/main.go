package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"agentflow/flow"
)

// sharedState has fields that multiple Parallel branches might write to.
// Without synchronization, concurrent writes cause a data race.
type sharedState struct {
	mu      sync.Mutex
	Results map[string]string
	Counter int64
}

// isolatedState is used with ParallelIsolated. Each branch gets its own copy,
// so there is no shared mutable state and no need for a mutex.
type isolatedState struct {
	BranchOutput string
	Combined     string
}

func main() {
	fmt.Println("=== Example A: Parallel with shared state (needs mutex) ===")
	runShared()

	fmt.Println("\n=== Example B: ParallelIsolated (lock-free) ===")
	runIsolated()
}

func runShared() {
	wf := flow.New[sharedState]("parallel-shared")

	wf.Task("init", func(_ context.Context, s *sharedState) error {
		s.Results = make(map[string]string)
		return nil
	}).Next("fan")

	// Parallel runs all branches concurrently and waits for all of them.
	// Branches share the same *State pointer, so concurrent writes to
	// overlapping fields MUST be synchronized.
	wf.Parallel("fan",
		flow.Branch[sharedState]{
			Name: "branch-a",
			Run: func(ctx context.Context, s *sharedState) error {
				// Simulate work. In a real tool you'd respect ctx.Done().
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				s.mu.Lock()
				s.Results["a"] = "output-from-a"
				atomic.AddInt64(&s.Counter, 1)
				s.mu.Unlock()
				return nil
			},
		},
		flow.Branch[sharedState]{
			Name: "branch-b",
			Run: func(ctx context.Context, s *sharedState) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				s.mu.Lock()
				s.Results["b"] = "output-from-b"
				atomic.AddInt64(&s.Counter, 2)
				s.mu.Unlock()
				return nil
			},
		},
	).Next("join")

	wf.Task("join", func(_ context.Context, s *sharedState) error {
		fmt.Printf("  [join] results: %v\n", s.Results)
		fmt.Printf("  [join] counter: %d\n", atomic.LoadInt64(&s.Counter))
		return nil
	}).Next("done")

	wf.Succeed("done")

	var state sharedState
	if _, err := wf.Run(context.Background(), &state); err != nil {
		fmt.Printf("  error: %v\n", err)
	}
}

func runIsolated() {
	wf := flow.New[isolatedState]("parallel-isolated")

	// copy creates a fresh state copy for each branch. Branches mutate their
	// own copy — no shared memory, no data race, no mutex.
	copyFn := func(s *isolatedState) *isolatedState {
		return &isolatedState{
			BranchOutput: s.BranchOutput,
			Combined:     s.Combined,
		}
	}

	// merge folds all branch copies back into the main state after the join.
	mergeFn := func(main *isolatedState, copies []*isolatedState) error {
		for _, c := range copies {
			main.Combined += c.BranchOutput + "; "
		}
		return nil
	}

	// ParallelIsolated(name, copy, merge, branches...)
	// Each branch receives its own *isolatedState copy.
	wf.ParallelIsolated("fan", copyFn, mergeFn,
		flow.Branch[isolatedState]{
			Name: "branch-a",
			Run: func(_ context.Context, s *isolatedState) error {
				s.BranchOutput = "result-from-a"
				return nil
			},
		},
		flow.Branch[isolatedState]{
			Name: "branch-b",
			Run: func(_ context.Context, s *isolatedState) error {
				s.BranchOutput = "result-from-b"
				return nil
			},
		},
	).Next("join")

	wf.Task("join", func(_ context.Context, s *isolatedState) error {
		fmt.Printf("  [join] combined: %s\n", s.Combined)
		return nil
	}).Next("done")

	wf.Succeed("done")

	var state isolatedState
	if _, err := wf.Run(context.Background(), &state); err != nil {
		fmt.Printf("  error: %v\n", err)
	}
}

/*
Key takeaways:

Shared-state Parallel:
- All branches receive the same *T pointer.
- Concurrent writes to the same fields are data races. Use sync.Mutex,
  sync/atomic, or channel-based coordination.
- A failing branch cancels in-flight siblings via context (new behavior).

ParallelIsolated:
- Each branch gets its own *T copy (produced by the `copy` function).
- Branches cannot race on shared fields — no mutex needed.
- After all branches finish, `merge` folds all copies back into main state.
- If any branch fails, in-flight siblings are cancelled and merge is skipped.
- Use ParallelIsolated when branches write to overlapping state and you
  want to avoid locking. Use Parallel when branches genuinely need to
  coordinate through shared memory.
*/