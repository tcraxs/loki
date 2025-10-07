package executor

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/semconv"
)

func NewProjectPipeline(input Pipeline, projection *physical.Projection, evaluator *expressionEvaluator) (*GenericPipeline, error) {
	return newGenericPipeline(Local, func(ctx context.Context, inputs []Pipeline) state {
		// Pull the next item from the input pipeline
		input := inputs[0]
		batch, err := input.Read(ctx)
		if err != nil {
			return nil, err
		}
		defer batch.Release()

		columns := projection.Expressions
		// short circuit if there are no columns to project, treat as a select *
		if len(columns) == 0 {
			projectedRecord := array.NewRecord(batch.Schema(), batch.Columns(), batch.NumRows())
			return projectedRecord, nil
		}

		columnOrder := []string{}
		projected := map[string]arrow.Array{}
		fields := map[string]arrow.Field{}

		for _, col := range columns {
			vec, err := evaluator.eval(col, batch)
			if err != nil {
				return nil, err
			}
			defer vec.Release()

			switch col := col.(type) {
			case *physical.ColumnExpr:
				ident := semconv.NewIdentifier(col.Ref.Column, vec.ColumnType(), vec.Type())
				columnName := ident.FQN()
				columnOrder = append(columnOrder, columnName)
				fields[columnName] = semconv.FieldFromIdent(ident, true)
				arr := vec.ToArray()
				defer arr.Release()
				projected[columnName] = arr
			case *physical.UnwrapExpr:
				if arrStruct, ok := vec.ToArray().(*array.Struct); ok {
					defer arrStruct.Release()
					for i := range arrStruct.NumField() {
						arr := arrStruct.Field(i)
						defer arr.Release()

						structSchema, ok := arrStruct.DataType().(*arrow.StructType)
						if !ok {
							return nil, fmt.Errorf("unexpected type for struct field %d, got %T", i, arrStruct.DataType())
						}
						field := structSchema.Field(i)

						if _, ok := fields[field.Name()]; ok {
							continue
						}

						columnOrder = append(columnOrder, field.Name())
						fields[field.Name()] = field
						projected[field.Name()] = arr
					}
				}
			default:
				return nil, fmt.Errorf("unknown expression: %v", col)
			}
		}

		// Evaluating the expressions gives us a set of columns to project.
		// If expanding (like in parse or unwrap), we add the new columns to the existing columns.
		// Otherwise, we replace the existing columns with the new columns.
		if projection.Expand {
			schema := batch.Schema()
			for i, field := range schema.Fields() {
				columnOrder = append(columnOrder, field.Name())
				fields[field.Name()] = field
				projected[field.Name()] = batch.Column(i)
			}
		}

		fieldsArr := make([]arrow.Field, 0, len(fields))
		projectedArr := make([]arrow.Array, 0, len(projected))
		for _, column := range columnOrder {
			fieldsArr = append(fieldsArr, fields[column])
			projectedArr = append(projectedArr, projected[column])
		}

		schema := arrow.NewSchema(fieldsArr, nil)
		projectedRecord := array.NewRecord(schema, projectedArr, batch.NumRows())
		return projectedRecord, nil
	}, input), nil
}
