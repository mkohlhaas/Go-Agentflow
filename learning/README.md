# agentflow Learning Project

A progressive, hands-on tutorial for the `agentflow` workflow engine.

Each numbered directory contains a complete, runnable Go program that teaches
one or more aspects of the library. Read the comments in each file — they
explain not just *how* the code works, but *why* the engine behaves that way.

## Prerequisites

- Go 1.24+ (the `agentflow` module uses generics heavily)
- This module lives inside the `agentflow` repo and imports the local package
  via a `replace` directive.

## How to run

From this directory:

```sh
# Run any example
go run ./01-basics
go run ./02-choice-loops
go run ./03-decisions
go run ./04-tool-loops
go run ./05-parallel
go run ./06-wait-cancellation
go run ./07-errors-retries
go run ./08-observability
go run ./09-putting-it-together
go run ./10-sub-workflows

# Run all tests (none here — these are examples, not tests)
go build ./...
go vet ./...
```

## What each example covers

| Directory | Concepts |
|-----------|----------|
| `01-basics` | `New`, `Task`, `Succeed`, `Next`, `Run`, typed state |
| `02-choice-loops` | `Choice`, `When`, `Otherwise`, bounded loops via graph cycles |
| `03-decisions` | `Decision`, `DecisionPolicy`, whitelisted route options, `MaxAttempts` |
| `04-tool-loops` | `ToolLoop`, `ToolLoopPolicy`, tools, `ToolLoopTurn` history, `Done` / `Fail` |
| `05-parallel` | `Parallel` (shared state + mutexes) vs `ParallelIsolated` (lock-free copies) |
| `06-wait-cancellation` | `Wait`, context cancellation, `StatusCanceled` |
| `07-errors-retries` | `WithRetry`, `Catch`, `MatchIs`, `MatchAs`, error routes |
| `08-observability` | `WithObserver`, `Result.Trace`, `Mermaid()` diagram export |
| `09-putting-it-together` | A complete mini-workflow combining several node types |
| `10-sub-workflows`       | Sub-workflows: basic usage, error handling, nesting |

## Suggested study order

Work through the directories in order. Each one builds on the previous.

After reading an example, try modifying it:
- Change a route target and watch `Run` fail validation
- Add a second branch to `Parallel` and see the race without a mutex
- Swap `Parallel` for `ParallelIsolated` and remove the mutex
- Make a `Decision` policy return an invalid key and observe `decision_rejected`
- Nest a sub-workflow inside another sub-workflow and watch the Mermaid diagram render nested subgraphs
