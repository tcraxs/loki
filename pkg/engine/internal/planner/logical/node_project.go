package logical

import (
	"fmt"
	"strings"

	"github.com/grafana/loki/v3/pkg/engine/internal/planner/schema"
)

// The Projection instruction projects (keeps/drops) columns from a relation.
// Projection implements both [Instruction] and [Value].
type Projection struct {
	id string

	Table       Value   // The input relation.
	Expressions []Value // The expressions to apply for projecting columns.

	Expand bool // Indicates that projected columns should be added to input relation
}

var (
	_ Value       = (*Projection)(nil)
	_ Instruction = (*Projection)(nil)
)

// Name returns an identifier for the Projection operation.
func (p *Projection) Name() string {
	if p.id != "" {
		return p.id
	}
	return fmt.Sprintf("%p", p)
}

// String returns the disassembled SSA form of the Projection instruction.
func (p *Projection) String() string {
	params := make([]string, 0, len(p.Expressions)+1)
	params = append(params, fmt.Sprintf("mode=%s", p.mode()))
	for _, expr := range p.Expressions {
		params = append(params, fmt.Sprintf("expr=%s", expr))
	}
	return fmt.Sprintf("PROJECT %s [%s]", p.Table.Name(), strings.Join(params, ", "))
}

// Schema returns the schema of the Projection plan.
func (p *Projection) Schema() *schema.Schema {
	// unused
	return nil
}

func (p *Projection) mode() string {
	var mode string
	if p.Expand {
		mode += "E"
	}
	return mode
}

func (p *Projection) isInstruction() {}
func (p *Projection) isValue()       {}
