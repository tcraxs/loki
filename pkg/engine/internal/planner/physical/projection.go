package physical

import "fmt"

// Projection represents a column selection operation in the physical plan.
// It contains a list of columns (column expressions) that are later
// evaluated against the input columns to remove unnecessary colums from the
// intermediate result.
type Projection struct {
	id string

	// Expressions is a set of column expressions that are used to drop not needed
	// columns that do not match the expression evaluation.
	Expressions []Expression

	// Expand indicates whether the projection should expand the table with the result of the expression.
	// For example, parse and unwrap expressions will expand the table with the new columns.
	// Whereas column expressions when projecting specific columns for the result will not expand the table.
	Expand bool
}

// ID implements the [Node] interface.
// Returns a string that uniquely identifies the node in the plan.
func (p *Projection) ID() string {
	if p.id == "" {
		return fmt.Sprintf("%p", p)
	}
	return p.id
}

// Type implements the [Node] interface.
// Returns the type of the node.
func (*Projection) Type() NodeType {
	return NodeTypeProjection
}

// Accept implements the [Node] interface.
// Dispatches itself to the provided [Visitor] v
func (p *Projection) Accept(v Visitor) error {
	return v.VisitProjection(p)
}
