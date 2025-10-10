package workflow

import (
	"fmt"

	"github.com/oklog/ulid/v2"

	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/util/dag"
)

// planner is responsible for constructing the Task graph held by a [Workflow].
type planner struct {
	Graph    dag.Graph[*Task]
	Physical *physical.Plan
}

// planWorkflow partitions a physical plan into a graph of tasks.
//
// planWorkflow returns an error if the provided physical plan does not
// have exactly one root node, or if the physical plan cannot be partitioned.
func planWorkflow(plan *physical.Plan) (dag.Graph[*Task], error) {
	root, err := plan.Root()
	if err != nil {
		return dag.Graph[*Task]{}, err
	}

	planner := &planner{Physical: plan}
	if err := planner.Process(root); err != nil {
		return dag.Graph[*Task]{}, err
	}

	return planner.Graph, nil
}

// Process builds a set of tasks from a root physical plan node. Built tasks are
// added to p.graph.
func (p *planner) Process(root physical.Node) error {
	_, err := p.processNode(root)
	return err
}

// processNode builds a set of tasks from the given node.
//
// The reuslting task is the task immediately produced by node, which callers
// can use to add edges. All tasks, including those produced in recursive calls
// to processNode, are added into p.Graph.
func (p *planner) processNode(node physical.Node) (*Task, error) {
	var (
		// taskPlan is the in-progress physical plan for an individual task.
		taskPlan dag.Graph[physical.Node]

		sources = make(map[physical.Node][]*Stream)

		// childrenTasks is the slice of immediate Tasks produced by processing
		// the children of node.
		childrenTasks []*Task
	)

	// Immediately add the node to the task physical plan.
	taskPlan.Add(node)

	var (
		stack    = make(stack[physical.Node], 0, p.Physical.Len())
		visited  = make(map[physical.Node]struct{}, p.Physical.Len())
		nodeTask = make(map[physical.Node]*Task, p.Physical.Len())
	)

	stack.Push(node)
	for stack.Len() > 0 {
		next := stack.Pop()
		if _, ok := visited[next]; ok {
			// Ignore nodes that have already been visited, which can happen
			// if a node has multiple parents.
			continue
		}
		visited[next] = struct{}{}

		for _, child := range p.Physical.Children(next) {
			// NOTE(rfratto): We may have already seen child before (if it has
			// more than one parent), but we want to continue processing to
			// ensure we update sources properly and retain all relationships
			// within the same graph.

			switch {
			case isPipelineBreaker(child):
				childTask, found := nodeTask[child]
				if !found {
					// Split the pipeline breaker into its own set of tasks.
					task, err := p.processNode(child)
					if err != nil {
						return nil, err
					}
					childrenTasks = append(childrenTasks, task)
					nodeTask[child] = task
					childTask = task
				}

				// Create a stream so we can read output from our child task,
				// then update the child task so that it writes to the stream
				// via its sinks.
				stream := &Stream{ULID: ulid.Make()}

				root, err := childTask.Fragment.Root()
				if err != nil {
					return nil, fmt.Errorf("unexpected child task with more than one root node: %w", err)
				}
				childTask.Sinks[root] = append(childTask.Sinks[root], stream)
				sources[next] = append(sources[next], stream)

			default:
				// Add child node into the plan (if we haven't already) and
				// retain existing edges.
				taskPlan.Add(child)
				_ = taskPlan.AddEdge(dag.Edge[physical.Node]{
					Parent: next,
					Child:  child,
				})

				stack.Push(child) // Push child for further processing.
			}
		}
	}

	task := &Task{
		ULID:     ulid.Make(),
		Fragment: physical.FromGraph(taskPlan),
		Sources:  sources,
		Sinks:    make(map[physical.Node][]*Stream),
	}
	p.Graph.Add(task)

	// Wire edges to children tasks and update their sinks to note sending to
	// our task.
	for _, child := range childrenTasks {
		_ = p.Graph.AddEdge(dag.Edge[*Task]{
			Parent: task,
			Child:  child,
		})
	}

	return task, nil
}

// isPipelineBreaker returns true if the node is a pipeline breaker.
func isPipelineBreaker(node physical.Node) bool {
	// TODO(rfratto): Should this information be exposed by the node itself? A
	// decision on this should wait until we're able to serialize the node over
	// the network, since that might impact how we're able to define this at the
	// node level.
	switch node.Type() {
	case physical.NodeTypeRangeAggregation, physical.NodeTypeVectorAggregation:
		return true
	}

	return false
}

// stack is a slice with Push and Pop operations.
type stack[E any] []E

func (s stack[E]) Len() int { return len(s) }

func (s *stack[E]) Push(e E) { *s = append(*s, e) }

func (s *stack[E]) Pop() E {
	if len(*s) == 0 {
		panic("stack is empty")
	}
	last := len(*s) - 1
	e := (*s)[last]
	*s = (*s)[:last]
	return e
}
