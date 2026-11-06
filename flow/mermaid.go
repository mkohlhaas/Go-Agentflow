package flow

import (
	"fmt"
	"strings"
)

// Mermaid renders the workflow as a deterministic Mermaid flowchart. Node
// declaration order is preserved, so repeated calls produce identical output.
func (w *Workflow[T]) Mermaid() string {
	ids := make(map[string]string, len(w.order))
	for i, name := range w.order {
		ids[name] = fmt.Sprintf("n%d", i)
	}

	var b strings.Builder
	b.WriteString("%% workflow: ")
	b.WriteString(oneLine(w.Name))
	b.WriteString("\nflowchart TD\n")

	startID := "start"
	if w.start != "" {
		b.WriteString("    start([\"start\"])\n")
	}

	for _, name := range w.order {
		node := w.nodes[name]
		id := ids[name]
		b.WriteString("    ")
		b.WriteString(nodeShape(id, nodeLabel(node), node.Kind))
		b.WriteByte('\n')
	}

	// Parallel branches are grouped in subgraphs. Branch nodes are synthetic:
	// they exist to show the fan-out and join in the diagram, not in the engine.
	for _, name := range w.order {
		node := w.nodes[name]
		if node.Kind != KindParallel {
			continue
		}
		id := ids[name]
		fmt.Fprintf(&b, "    subgraph %s_par[\"%s\"]\n", id, escapeLabel("parallel branches"))
		b.WriteString("        direction TB\n")
		for i, branch := range node.Branches {
			fmt.Fprintf(&b, "        %s_b%d[\"%s\"]\n", id, i, escapeLabel("branch: "+branch.Name))
		}
		b.WriteString("    end\n")
	}

	if w.start != "" {
		fmt.Fprintf(&b, "    %s --> %s\n", startID, ids[w.start])
	}

	for _, name := range w.order {
		node := w.nodes[name]
		id := ids[name]
		switch node.Kind {
		case KindChoice, KindDecision:
			for _, key := range node.RouteKeys {
				label := key
				if key == DefaultRoute {
					label = "otherwise"
				}
				b.WriteString(edge(id, ids[node.Routes[key]], label, false))
			}
		case KindParallel:
			for i := range node.Branches {
				fmt.Fprintf(&b, "    %s --> %s_b%d\n", id, id, i)
				if node.NextNode != "" {
					fmt.Fprintf(&b, "    %s_b%d --> %s\n", id, i, ids[node.NextNode])
				}
			}
		case KindSucceed, KindFail:
			// terminal
		default:
			if node.NextNode != "" {
				b.WriteString(edge(id, ids[node.NextNode], "", false))
			}
		}
		for _, route := range node.ErrorRoutes {
			b.WriteString(edge(id, ids[route.Target], "error", true))
		}
	}

	b.WriteString("\n")
	for _, name := range w.order {
		node := w.nodes[name]
		fmt.Fprintf(&b, "    class %s %s;\n", ids[name], classFor(node.Kind))
		if node.Kind == KindParallel {
			for i := range node.Branches {
				fmt.Fprintf(&b, "    class %s_b%d branch;\n", ids[name], i)
			}
		}
	}

	b.WriteString("\n")
	b.WriteString(`    classDef task fill:#eef2ff,stroke:#4f46e5,color:#1e1b4b;` + "\n")
	b.WriteString(`    classDef choice fill:#fef3c7,stroke:#d97706,color:#78350f;` + "\n")
	b.WriteString(`    classDef decision fill:#ffedd5,stroke:#ea580c,color:#7c2d12;` + "\n")
	b.WriteString(`    classDef parallel fill:#f3e8ff,stroke:#9333ea,color:#4c1d95;` + "\n")
	b.WriteString(`    classDef wait fill:#f1f5f9,stroke:#64748b,color:#1e293b;` + "\n")
	b.WriteString(`    classDef agent fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;` + "\n")
	b.WriteString(`    classDef subworkflow fill:#f0fdf4,stroke:#15803d,color:#14532d;` + "\n")
	b.WriteString(`    classDef succeed fill:#dcfce7,stroke:#16a34a,color:#14532d;` + "\n")
	b.WriteString(`    classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d;` + "\n")
	b.WriteString(`    classDef branch fill:#faf5ff,stroke:#a855f7,color:#581c87;` + "\n")
	return b.String()
}

func nodeShape(id, label string, kind NodeKind) string {
	switch kind {
	case KindChoice:
		return fmt.Sprintf("%s{\"%s\"}", id, label)
	case KindDecision:
		return fmt.Sprintf("%s{{\"%s\"}}", id, label)
	case KindSubWorkflow:
		return fmt.Sprintf("%s[[\"%s\"]]", id, label)
	default:
		return fmt.Sprintf("%s[\"%s\"]", id, label)
	}
}

func nodeLabel[T any](node *Node[T]) string {
	kind := typeLabel(node)
	if kind != "" {
		return escapeLabel(node.Name) + "<br/><small>" + escapeLabel(kind) + "</small>"
	}
	return escapeLabel(node.Name)
}

func typeLabel[T any](node *Node[T]) string {
	switch node.Kind {
	case KindTask:
		if node.Retry != nil {
			return fmt.Sprintf("task / retry x%d", node.Retry.MaxAttempts)
		}
		return "task"
	case KindChoice:
		return "choice"
	case KindDecision:
		if node.Decision != nil {
			return fmt.Sprintf("decision / %d options", len(node.Decision.Options))
		}
		return "decision"
	case KindParallel:
		return fmt.Sprintf("parallel / %d branches", len(node.Branches))
	case KindWait:
		return "wait " + node.Delay.String()
	case KindAgent:
		if node.Agent != nil {
			return fmt.Sprintf("agent loop / %d tools", len(node.Agent.Tools))
		}
		return "agent loop"
	case KindSubWorkflow:
		if node.SubWorkflow != nil {
			return fmt.Sprintf("sub-workflow / %d nodes", len(node.SubWorkflow.order))
		}
		return "sub-workflow"
	case KindSucceed:
		return "succeed"
	case KindFail:
		return "fail"
	default:
		return string(node.Kind)
	}
}

func classFor(kind NodeKind) string {
	switch kind {
	case KindTask:
		return "task"
	case KindChoice:
		return "choice"
	case KindDecision:
		return "decision"
	case KindParallel:
		return "parallel"
	case KindWait:
		return "wait"
	case KindAgent:
		return "agent"
	case KindSubWorkflow:
		return "subworkflow"
	case KindSucceed:
		return "succeed"
	case KindFail:
		return "fail"
	default:
		return "task"
	}
}

func edge(from, to, label string, dotted bool) string {
	arrow := "-->"
	if dotted {
		arrow = "-.->"
	}
	if label == "" {
		return fmt.Sprintf("    %s %s %s\n", from, arrow, to)
	}
	return fmt.Sprintf("    %s %s|\"%s\"| %s\n", from, arrow, escapeLabel(label), to)
}

// escapeLabel protects user text for a quoted Mermaid label. Markup such as
// <br/> is added by the renderer itself and is not passed through here.
func escapeLabel(s string) string {
	replacer := strings.NewReplacer(
		"&", "#amp;",
		"\"", "#quot;",
		"<", "#lt;",
		">", "#gt;",
		"\n", " ",
		"\r", " ",
	)
	return replacer.Replace(s)
}

func oneLine(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ").Replace(s)
}
