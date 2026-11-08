# Design patterns and best practices

`agentflow` is deliberately small: a static graph, a typed state value and a
handful of node kinds. Most of the design work is therefore not in the engine
but in how you shape the graph. This guide collects the patterns that work well
with this library, the trade-offs behind them, and the mistakes that are easy to
make.

It assumes you have read the [README](../README.md) and preferably worked
through the [learning examples](../learning/README.md). The examples are the
long-form version of several patterns below; this document links to them rather
than repeating their code.

## Overview

- [Choosing the right node](#choosing-the-right-node)
- [Patterns](#patterns)
  - [Linear pipeline](#linear-pipeline)
  - [Propose and verify](#propose-and-verify)
  - [Bounded repair loop](#bounded-repair-loop)
  - [Whitelisted LLM routing](#whitelisted-llm-routing)
  - [Tool-using agent](#tool-using-agent)
  - [Fan-out / fan-in](#fan-out--fan-in)
  - [ForEach workarounds](#foreach-workarounds)
  - [Reusable subflow](#reusable-subflow)
  - [Retry plus fallback](#retry-plus-fallback)
  - [Approval gate](#approval-gate)
  - [Audit and observability](#audit-and-observability)
  - [Docs as code](#docs-as-code)
- [State design](#state-design)
- [Policy design](#policy-design)
- [Testing workflows](#testing-workflows)
- [Anti-patterns](#anti-patterns)
- [Production checklist](#production-checklist)

## Choosing the right node

Start with the smallest node that expresses the behaviour. A graph with five
obvious nodes is easier to review than one with two clever ones.

| Need | Use | Why |
| --- | --- | --- |
| Run one piece of Go code | `Task` | Explicit, testable, retryable. |
| Branch on a condition you can compute | `Choice` | Deterministic and fully testable. |
| Let an LLM choose from declared routes | `Decision` | Options are whitelisted; invalid choices are rejected. |
| Let an LLM choose tools iteratively | `ToolLoop` | Bounded policy/tool turns with a transcript. |
| Run fixed branches concurrently | `Parallel` | Waits for all branches and joins. |
| Run branches that write the same fields | `ParallelIsolated` | Per-branch state copies, then an explicit merge. |
| Reuse a multi-node graph | `SubWorkflow` | Shares state, nests, validates as one unit. |
| Repeat a multi-node body until a condition | Graph cycle today | Use a cycle plus `Choice`; a dedicated `Loop` node does not exist. |
| Iterate over runtime data | Graph cycle or worker pool | `Parallel` needs a static branch list; see [ForEach workarounds](#foreach-workarounds). |
| Delay or expose a cancellation point | `Wait` | Context-aware sleep. |
| End successfully | `Succeed` | `StatusSucceeded`. |
| End with a deliberate business failure | `Fail` | `StatusBusinessFailed`, distinct from an unhandled error. |
| Recover from an expected error | `Catch` | Routes matching errors instead of failing the run. |
| Retry a flaky operation | `WithRetry` on a `Task` | Bounded attempts with backoff. |

Two rules of thumb:

- **Use `Choice` unless a model genuinely has to decide.** A deterministic
  branch is cheaper, faster and testable without fakes.
- **Use `ToolLoop` only when the sequence of actions is unknown.** For a fixed
  sequence, chain `Task` nodes. A fixed pipeline hidden inside a policy is
  harder to inspect, bound and test.

## Patterns

Each entry describes the problem, the graph shape, when to reach for it, and the
pitfalls specific to this engine.

### Linear pipeline

**Problem:** a fixed sequence of steps where each step depends on the previous
one.

**Shape:** `Task → Task → Task → Succeed`, linked with `Next`.

**When:** the order and number of steps are known at build time. This is the
default shape; most workflows are a pipeline with one or two branches.

**Pitfalls:**

- Do not reach for `Decision` just because an LLM is available. If a step is
  deterministic, keep it deterministic.
- Give each `Task` one responsibility. A task that builds, tests and reports is
  three tasks wearing a trench coat.
- Remember that `WithRetry` retries the whole task. Retrying a non-idempotent
  task can duplicate side effects.

See [`learning/01-basics`](../learning/01-basics/main.go).

### Propose and verify

**Problem:** a model proposes a change, but you do not want the model to be the
judge of whether the change is correct.

**Shape:** `Decision` or `ToolLoop` proposes → deterministic `Task` verifies →
`Choice` gates the result.

```text
prepare → decide(fix, stop) → repair → verify → gate(pass, otherwise) → done / fail
```

**When:** coding agents, config changes, migrations — anything with a cheap,
objective check such as `go build`, `go test` or a schema validation.

**Pitfalls:**

- The verifier must read the real result, not a flag the model set. Run the
  command and record its exit status.
- Keep the gate separate from the proposal. Combining them lets a confident
  model skip verification.
- When verification fails, route to a bounded repair loop or a `Fail` node. Do
  not silently continue.

See the [coding-agent example](../examples/codingagent/main.go) and
[`learning/09-putting-it-together`](../learning/09-putting-it-together/main.go).

### Bounded repair loop

**Problem:** repeat a fix-and-check body until it succeeds or a budget is
exhausted.

**Shape:** a graph cycle with a counter in state and a `Choice` that decides
whether to loop.

```go
wf.Task("repair", repair).Next("verify")
wf.Task("verify", verify).Next("check")
wf.Choice("check", func(ctx context.Context, s *State) (string, error) {
    if s.TestsPassed || s.Attempts >= 3 {
        return "", nil
    }
    s.Attempts++
    return "again", nil
}).When("again", "repair").Otherwise("report")
```

**When:** the body is more than a single task, or you need per-iteration trace
events and error routes.

**Pitfalls:**

- Bound the loop in state, not only with `MaxSteps`. `ErrMaxSteps` is a blunt
  instrument; a local counter lets you fail gracefully with a useful message.
- Increment the counter only after you know whether the iteration should repeat.
  Incrementing before the work can skip an attempt or misreport the count.
- Keep the loop body deterministic where possible. A cycle containing a
  `Decision` is fine, but each extra model call multiplies cost.
- Validation rejects a reachable cycle that cannot reach a terminal node, so the
  loop must always have a path to `Succeed` or `Fail`.

See [`learning/02-choice-loops`](../learning/02-choice-loops/main.go).

### Whitelisted LLM routing

**Problem:** the model should choose among a small set of known next steps, not
invent workflow structure.

**Shape:** one `Decision` node with explicit `RouteOption`s.

```go
wf.Decision("triage", flow.DecisionSpec[State]{
    Options: []flow.RouteOption{
        {Key: "fix", Description: "apply the proposed fix", Target: "repair"},
        {Key: "stop", Description: "abort the run", Target: "give-up"},
    },
    Choose:      policy,
    MaxAttempts: 2,
})
```

**When:** the set of routes is finite and understandable. This is the safest way
to give a model control over control flow.

**Pitfalls:**

- Write descriptions for the model, not for the diagram. The description is part
  of the prompt surface.
- Always declare a safe fallback such as `stop`. An invalid answer is retried
  and then fails the node.
- Do not encode data in route keys. Keys are identifiers; put data in state.
- `MaxAttempts` protects against malformed output, not against bad judgement. A
  valid but unwise choice still needs a deterministic gate downstream.

See [`learning/03-decisions`](../learning/03-decisions/main.go).

### Tool-using agent

**Problem:** the next action depends on observations from previous actions, so
the sequence cannot be declared in advance.

**Shape:** one `ToolLoop` node with a policy and a small tool allowlist.

**When:** debugging, exploration, or any task where the model must react to
command output. The policy sees the accumulated `[]ToolLoopTurn`, including
errors and rejected tools.

**Pitfalls:**

- Keep the tool list small and purpose-built. Every tool is attack surface and
  prompt surface.
- Validate tool arguments inside the tool. `map[string]string` is convenient,
  not safe; do not let a tool escape its workspace root.
- Return useful observations. A tool that returns `"ok"` gives the policy
  nothing to correct against.
- Treat `Fail` as a last resort and `Done` as a claim, not proof. Verify
  important outcomes with a `Choice` gate after the loop.
- Set `MaxIterations` for the workload. The default is
  `flow.DefaultMaxToolLoopIterations` (8); a real agent may need more, a narrow
  one less.

See [`learning/04-tool-loops`](../learning/04-tool-loops/main.go).

### Fan-out / fan-in

**Problem:** run several independent pieces of work concurrently, then continue
once with the combined result.

**Shape:** a `Parallel` node with one branch per unit of work, followed by
`Next`.

Choose between two variants:

| Variant | State model | Use when |
| --- | --- | --- |
| `Parallel` | All branches share `*T`. | Branches touch disjoint fields, or you add a mutex. |
| `ParallelIsolated` | Each branch gets a copy; `merge` folds copies back. | Branches write overlapping fields and copying is cheap. |

**Pitfalls:**

- Shared-state `Parallel` is a data race waiting to happen. If two branches
  append to the same slice or increment the same counter, either lock or use
  `ParallelIsolated`.
- `ParallelIsolated` is not free: `copy` must be a deep copy, and `merge` is
  where correctness lives. A shallow copy that shares a map still races.
- The first error in declaration order is returned and cancels in-flight
  siblings. If you need all errors, collect them in state under a lock and
  return `nil` from the branch.
- Branch events name branches, not work items. If per-item identity matters,
  name the branches after the items or use a graph cycle instead.

See [`learning/05-parallel`](../learning/05-parallel/main.go).

### ForEach workarounds

**Problem:** you need to run the same body once per element of a collection.
There is no `ForEach` node, and `Parallel` takes a static branch list.

The right workaround depends on when the collection is known and whether you
need concurrency.

**1. Sequential graph cycle (default choice).** Keep an index in state, process
one item per visit, and let a `Choice` decide whether to continue.

```go
wf.Task("process-next", processNext).Next("check")
wf.Choice("check", func(ctx context.Context, s *State) (string, error) {
    if s.Index < len(s.Items) {
        return "again", nil
    }
    return "", nil
}).When("again", "process-next").Otherwise("done")
```

Use this when you need per-item trace events, retries or error routes. Advance
the index only after the item succeeds. Budget roughly two steps per item
against `MaxSteps`.

**2. One `Task` with an internal loop.** Simplest, but the loop is invisible to
the graph and the trace. Use it for small or cheap collections where per-item
observability does not matter. `WithRetry` here retries the whole batch.

**3. Static `Parallel` fan-out.** If the items are known when the graph is
built, create one branch per item. You get real concurrency and branch events at
the cost of a graph whose size depends on the data. Write results by index so
completion order does not affect output.

**4. Worker pool over `Parallel`.** If the collection is runtime data but you
need concurrency, create a fixed number of worker branches that pull from a
shared index under a mutex. This bounds concurrency and keeps the graph static.
Error handling is coarse: returning an error cancels the other workers, so
collect per-item failures in state if you need them all.

**5. Nested `Run` inside a `Task`.** Technically possible, but discouraged. The
inner workflow is built dynamically, its trace is not part of the parent trace,
and validation runs per item. Use it only when no other shape fits.

Do not model large collections as a static `Parallel` with thousands of
branches; the diagram, spec and validation cost grow with the branch count.

### Reusable subflow

**Problem:** the same multi-node body appears in several places, or a workflow
is getting too large to read.

**Shape:** build a separate `Workflow[T]`, then attach it with
`SubWorkflow(name, sub).Next(...)`.

**When:** the body is a coherent unit with its own entry and exit, and sharing
state is acceptable. Sub-workflows nest and are validated as part of the parent.

**Pitfalls:**

- A sub-workflow shares the parent state. Namespacing state fields is your
  responsibility.
- `SubWorkflow` counts as one opaque step against the parent's `MaxSteps`, but
  it has its own internal step budget. Both can fail independently.
- A failing sub-workflow returns `*flow.SubWorkflowError` with the inner trace.
  Use `flow.MatchAs(&flow.SubWorkflowError{})` on a `Catch` route to inspect it.
- A sub-workflow may be shared by several parents, but it may not contain
  itself. Cycles are rejected during validation rather than overflowing the
  stack.

See [`learning/10-sub-workflows`](../learning/10-sub-workflows/main.go).

### Retry plus fallback

**Problem:** an operation is flaky, and even after retries you want a defined
fallback instead of a failed run.

**Shape:** `Task.WithRetry(...)` followed by `Catch(target, matcher)`.

```go
wf.Task("build", goBuild).
    WithRetry(flow.RetryPolicy{
        MaxAttempts:     3,
        InitialInterval: 100 * time.Millisecond,
        Backoff:         2,
        MaxInterval:     time.Second,
    }).
    Catch("fallback", flow.MatchIs(errBuildTimeout)).
    Next("test")
```

**When:** transient failures with a meaningful recovery path. Use
`MatchIs` for sentinel errors and `MatchAs` for error types.

**Pitfalls:**

- Retries apply to `Task` nodes only. A `ToolLoop` or `Decision` has its own
  attempt limits.
- `MaxAttempts` includes the first attempt, so `1` means no retry.
- Catch routes are checked in declaration order. Put specific matchers before a
  nil catch-all.
- Context cancellation is never routed through `Catch`; it ends the run as
  `StatusCanceled`.
- Do not use retries to paper over a business failure. If the correct response
  is a different route, model that with `Choice` or `Fail`.

See [`learning/07-errors-retries`](../learning/07-errors-retries/main.go).

### Approval gate

**Problem:** a human must approve an action before the workflow continues.

**Shape:** there is no human-in-the-loop node. Model approval as an external
signal read inside a `Task`, or as a `Decision` policy backed by your approval
system.

```go
wf.Task("await-approval", func(ctx context.Context, s *State) error {
    approved, err := approvals.Wait(ctx, s.ChangeID)
    s.Approved = approved
    return err
}).Next("gate")

wf.Choice("gate", func(ctx context.Context, s *State) (string, error) {
    if s.Approved {
        return "apply", nil
    }
    return "", nil
}).When("apply", "apply").Otherwise("rejected")
```

**When:** deploys, destructive migrations, spending decisions — anything with an
external authority.

**Pitfalls:**

- Use the context for the wait. An approval gate that ignores cancellation can
  hang a run forever.
- Record the approval decision in state and in the trace; do not rely on a
  boolean that the reviewer cannot audit.
- Do not implement approval as a `Wait` with a fixed duration. That is a guess,
  not an approval.

### Audit and observability

**Problem:** you need to understand what happened during a run, not just whether
it succeeded.

**Shape:** use `WithObserver` for live events and `Result.Trace` for the full
record.

```go
result, err := wf.Run(ctx, &state,
    flow.WithMaxSteps(200),
    flow.WithObserver(func(e flow.Event) {
        log.Printf("%s node=%s tool=%s", e.Kind, e.Node, e.Tool)
    }),
)
```

**When:** always, once a workflow does anything expensive or irreversible.

**Pitfalls:**

- The observer runs on the goroutine driving the run, not inside parallel
  branches, so it does not need its own locking. Do not block it for long.
- Traces are in memory. Export them if you need a durable audit log.
- Never log secrets, tokens or full model prompts into state or events. Redact
  at the tool or policy boundary.
- `Mermaid()` describes the declared graph, not the run. Use the trace for
  runtime behaviour and the diagram for review.

See [`learning/08-observability`](../learning/08-observability/main.go).

### Docs as code

**Problem:** diagrams and specs drift from the real graph.

**Shape:** generate both from the workflow and check them in, or render them in
CI.

```sh
go run ./examples/codingagent -mermaid > docs/coding-agent.mmd
go run ./examples/codingagent -spec    > docs/coding-agent.json
```

**When:** the graph is part of the design review, or external tooling consumes
the spec.

**Pitfalls:**

- Both `Mermaid()` and `MarshalSpec()` are deterministic. Regenerate rather than
  hand-edit.
- The spec is structure-only: it is not executable and cannot be round-tripped
  into a workflow.
- Function references are best-effort. Anonymous closures are omitted, so an
  empty `run`, `choose` or `policy` field means "not stably nameable", not
  "missing implementation".
- `Spec()` and `Mermaid()` describe the declared graph; they do not validate it.
  Call `Validate()` (or `Run`) to catch graph errors.

See [Exporting a workflow specification](../README.md#exporting-a-workflow-specification).

## State design

The state value is the workflow's only shared memory. Its shape determines how
easy the graph is to reason about.

**Keep one explicit state type.** `flow.New[State]` is generic for a reason:
every node, policy and tool receives `*State`. Avoid `any`, maps of strings, or
a second side channel. If a value matters across nodes, put it in state where
the trace and tests can see it.

**Separate the three kinds of data.**

- *Inputs* set before `Run`: the request, workspace path, configuration.
- *Working data*: current index, attempt count, intermediate results.
- *Outputs*: the final answer, status flags, artifacts.

Mixing them makes it hard to tell what a run produced versus what it consumed.

**Prefer explicit counters over inferred progress.** A repair loop should carry
`Attempts int`, not deduce the attempt count from the length of a log. Counters
are easy to bound and easy to assert in tests.

**Advance progress only after success.** In an item loop, increment the index
after the item is processed. If the task fails, the same item is retried or
routed instead of being silently skipped.

**Make retried work idempotent.** `WithRetry` may run a `Task` more than once.
Writing a file is usually fine; appending to a log or charging a card is not.
If a task cannot be idempotent, do not put it behind an automatic retry.

**Design for parallel access before you need it.** For shared-state `Parallel`,
decide which fields each branch owns. For `ParallelIsolated`, write a real deep
copy and a merge that is order-independent. Results indexed by branch or item
are the simplest safe shape.

**Keep copies cheap.** `ParallelIsolated` copies the whole state per branch. If
state contains a large buffer, either move the buffer out of the copied struct
or use shared state with locking.

**Check the context in long work.** State does not stop a runaway loop;
`ctx.Err()` does. Tools that shell out should use `exec.CommandContext`, and
long Go loops should check cancellation between items.

## Policy design

A policy is an adapter between the engine and a model. The engine's guarantees
depend on the policy behaving as a pure decision function.

**Treat model output as untrusted.** The engine validates route keys and tool
names, but it cannot validate intent. Parse responses strictly, reject
unexpected fields, and never execute a model-supplied command outside a tool.

**Never let a policy edit the graph.** Policies return a key or a tool decision.
They cannot add nodes, change routes or bypass a gate. Keep it that way; a policy
that mutates workflow structure defeats the point of the engine.

**Always provide a deterministic fallback.** A `Decision` needs a safe route such
as `stop`. A `ToolLoop` policy needs a path to `Fail` or `Done`. The fallback
should not depend on another model call.

**Bound every loop twice.** `MaxIterations` bounds one `ToolLoop`; `MaxSteps`
bounds graph cycles. Set both for the workload rather than relying on defaults.

**Keep prompts out of the engine.** Prompt construction, model selection, token
budgets and retries belong to the policy implementation. The engine passes typed
state and declared options; it does not know what a prompt is.

**Return errors for infrastructure failures, decisions for business outcomes.**
A network error is an `error`. "The tests still fail" is state plus a tool
observation. Mixing the two turns recoverable outcomes into failed runs.

**Prefer deterministic policies in tests and examples.** A fake policy that
returns a fixed sequence exercises the same interface as a real model and makes
the workflow reproducible.

**Log decisions, not reasoning.** Store the chosen key or tool and a short
justification. Full chain-of-thought is large, sensitive and not needed to audit
control flow. `ToolLoopTurn.Thought` is for a brief rationale, not a transcript
dump.

## Testing workflows

A workflow is ordinary Go code, so it can be tested without running a model.

**Use fake policies and fake tools.** Implement `DecisionPolicy` or
`ToolLoopPolicy` with a struct that returns a fixed answer or follows a scripted
sequence. Replace shell commands with functions that record calls and return
controlled results.

**Assert state, status and trace.** A useful test checks:

- the final `Status` (`succeeded`, `business-failed`, `failed`, `canceled`),
- the relevant fields of `Result.State`,
- the sequence of node names or event kinds in `Result.Trace`.

```go
result, err := wf.Run(context.Background(), &state)
if err != nil {
    t.Fatalf("run: %v", err)
}
if result.Status != flow.StatusSucceeded {
    t.Fatalf("status = %s", result.Status)
}
if !state.TestsPassed {
    t.Fatal("verification did not run")
}
```

**Test the unhappy paths.** At minimum:

- retry exhaustion and the matching `Catch` route,
- an invalid `Decision` key and `MaxAttempts` exhaustion,
- a `ToolLoop` that hits `MaxIterations`,
- a failing parallel branch,
- context cancellation (`StatusCanceled`),
- a sub-workflow failure surfaced as `*SubWorkflowError`.

**Test the graph, not just the functions.** A destination typo or an unreachable
node is caught by `Validate`, which `Run` calls first. A test that only calls
node functions directly will miss those mistakes.

**Keep one deterministic test per non-deterministic path.** Even if production
uses a real model, a scripted policy test should cover the route where the model
chooses badly.

**Use the spec and diagram in review.** `MarshalSpec()` and `Mermaid()` make
structural changes visible in a diff. Regenerate them in the same change that
edits the graph.

**Run the race detector.** `go test -race ./...` is the cheapest way to catch a
shared-state `Parallel` mistake. It will not catch every logical race, but it
catches the common ones.

## Anti-patterns

**One giant `Task` that hides the workflow.** If a task contains branching,
retries and a loop, it should be several nodes. You lose trace events, error
routes and a meaningful diagram.

**Unbounded loops.** A graph cycle without a state counter, or a `ToolLoop`
without `MaxIterations`, relies on `MaxSteps` to stop it. That produces a
generic `ErrMaxSteps` instead of a useful failure. Bound the loop where the
loop is defined.

**Unbounded collections under the default budget.** A sequential item loop costs
roughly two steps per item. A thousand items will exhaust `DefaultMaxSteps`.
Either raise the limit deliberately or batch.

**Using `ToolLoop` for a fixed pipeline.** If the steps never change, write
`Task` nodes. A policy that always returns the same sequence is a pipeline with
extra indirection and worse visibility.

**Using `Decision` for deterministic branching.** If the answer is computable in
Go, use `Choice`. A model call adds latency, cost and a failure mode for no
benefit.

**Mutating shared state from parallel branches without synchronisation.** This
is a data race. Use a mutex, atomics, or `ParallelIsolated` with a real deep
copy.

**Retrying a business failure.** `WithRetry` is for transient errors. If the
operation will never succeed, retrying delays the inevitable and hides the real
route.

**Treating the JSON spec as executable.** The spec describes structure, not
behaviour. It cannot be loaded back into a workflow and does not include the
logic of functions or policies.

**Expecting stable names for anonymous closures.** The spec omits them because
compiler-generated names change. If you need a function in the spec, give it a
name at package scope.

**Building graphs during a run.** The graph is meant to be static. Creating
workflows dynamically inside a `Task` hides structure from validation, the
diagram and the spec. If you need a repeated body, use a sub-workflow or a
cycle.

**Treating `Done` as verification.** A policy saying it is finished is a claim.
If correctness matters, verify with deterministic code and gate on the result.

## Production checklist

Before a workflow handles real work:

- [ ] `MaxSteps` is set for the expected graph size, and every cycle has a state
      counter with a graceful failure path.
- [ ] Every `ToolLoop` sets `MaxIterations` appropriate to the task.
- [ ] Every `Decision` has a safe fallback route and a sensible `MaxAttempts`.
- [ ] Every policy treats model output as untrusted and validates it.
- [ ] Tool arguments are validated, and tools are confined to a workspace root.
- [ ] Context timeouts and cancellation reach tools, subprocesses and long
      loops.
- [ ] Expected failures have a `Catch` route; unexpected failures produce
      `StatusFailed`.
- [ ] `Fail` is used for deliberate business outcomes, not for crashes.
- [ ] Shared-state `Parallel` writes are synchronised, or `ParallelIsolated` is
      used with a correct deep copy and merge.
- [ ] Retried tasks are idempotent.
- [ ] State contains no secrets, and traces/prompts are redacted.
- [ ] An observer or trace sink is configured where audit is required.
- [ ] Tests cover success, retry exhaustion, error routes, cancellation,
      iteration limits and parallel failure.
- [ ] `go test -race ./...` and `go vet ./...` pass.
- [ ] `Mermaid()` and `MarshalSpec()` artifacts are regenerated.
