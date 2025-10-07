package executor

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/dustin/go-humanize"

	"github.com/grafana/loki/v3/pkg/engine/internal/types"
)

func unwrapFn(operation types.UnaryOp) UnaryFunction {
	//TODO: change signature of Evaluate to add this alloc parameter
	alloc := memory.DefaultAllocator
	return UnaryFunc(func(input ColumnVector) (ColumnVector, error) {
		sourceCol, ok := input.ToArray().(*array.String)
		if !ok {
			return nil, fmt.Errorf("expected column to be of type string, got %T", input.ToArray())
		}

		// Get conversion function and process values
		conversionFn := getConversionFunction(operation)
		unwrappedCol, errTracker := convertValues(sourceCol, conversionFn, alloc)

		// Build error columns if needed
		errorCol, errorDetailsCol := errTracker.buildArrays()
		defer errTracker.releaseBuilders()

		// Build output schema and record
		fields := buildOutputFields(errTracker.hasErrors)
		arr, err := buildResult(unwrappedCol, errorCol, errorDetailsCol, fields)
		if err != nil {
			return nil, err
		}
		return &ArrayStruct{
			array: arr,
			ct:    types.ColumnTypeGenerated,
			rows:  input.Len(),
		}, nil
	})
}

type conversionFn func(value string) (float64, error)

func getConversionFunction(operation types.UnaryOp) conversionFn {
	switch operation {
	case types.UnaryOpUnwrapBytes:
		return convertBytes
	case types.UnaryOpUnwrapDuration:
		return convertDuration
	default:
		return convertFloat
	}
}

func convertValues(
	sourceCol *array.String,
	conversionFn conversionFn,
	allocator memory.Allocator,
) (arrow.Array, *errorTracker) {
	unwrappedBuilder := array.NewFloat64Builder(allocator)
	defer unwrappedBuilder.Release()

	tracker := newErrorTracker(allocator)

	for i := 0; i < sourceCol.Len(); i++ {
		if sourceCol.IsNull(i) {
			unwrappedBuilder.AppendNull()
			tracker.recordSuccess()
		} else {
			valueStr := sourceCol.Value(i)
			if val, err := conversionFn(valueStr); err == nil {
				unwrappedBuilder.Append(val)
				tracker.recordSuccess()
			} else {
				// Use 0.0 as default for errors, for backwards compatibility with old engine
				unwrappedBuilder.Append(0.0)
				tracker.recordError(i, err)
			}
		}
	}

	return unwrappedBuilder.NewArray(), tracker
}

func buildOutputFields(
	hasErrors bool,
) []arrow.Field {
	fields := make([]arrow.Field, 0, 3)

	// Add value field
	fields = append(fields, arrow.Field{
		Name: types.ColumnNameGeneratedValue,
		Type: arrow.PrimitiveTypes.Float64,
		Metadata: types.ColumnMetadata(
			types.ColumnTypeGenerated,
			types.Loki.Float,
		),
		Nullable: true,
	})

	// Add error fields if needed
	if hasErrors {
		fields = append(fields,
			arrow.Field{
				Name: types.ColumnNameError,
				Type: arrow.BinaryTypes.String,
				Metadata: types.ColumnMetadata(
					types.ColumnTypeParsed,
					types.Loki.String,
				),
				Nullable: true,
			},
			arrow.Field{
				Name: types.ColumnNameErrorDetails,
				Type: arrow.BinaryTypes.String,
				Metadata: types.ColumnMetadata(
					types.ColumnTypeParsed,
					types.Loki.String,
				),
				Nullable: true,
			},
		)
	}

	return fields
}

func buildResult(
	unwrappedCol, errorCol, errorDetailsCol arrow.Array,
	fields []arrow.Field,
) (*array.Struct, error) {
	hasErrors := errorCol != nil

	totalCols := 1
	if hasErrors {
		totalCols += 2
	}

	columns := make([]arrow.Array, totalCols)

	// Add new columns - these are newly created so don't need extra retain
	columns[0] = unwrappedCol
	if hasErrors {
		columns[1] = errorCol
		columns[2] = errorDetailsCol
	}

	// NewStructArrayWithFields will retain all columns
	result, err := array.NewStructArrayWithFields(columns, fields)

	// Release our references to newly created columns (result now owns them)
	unwrappedCol.Release()
	if hasErrors {
		errorCol.Release()
		errorDetailsCol.Release()
	}

	return result, err
}

func convertFloat(v string) (float64, error) {
	return strconv.ParseFloat(v, 64)
}

func convertDuration(v string) (float64, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	return d.Seconds(), nil
}

func convertBytes(v string) (float64, error) {
	b, err := humanize.ParseBytes(v)
	if err != nil {
		return 0, err
	}
	return float64(b), nil
}

type errorTracker struct {
	hasErrors      bool
	errorBuilder   *array.StringBuilder
	detailsBuilder *array.StringBuilder
	allocator      memory.Allocator
}

func newErrorTracker(allocator memory.Allocator) *errorTracker {
	return &errorTracker{allocator: allocator}
}

func (et *errorTracker) recordError(rowIndex int, err error) {
	if !et.hasErrors {
		et.errorBuilder = array.NewStringBuilder(et.allocator)
		et.detailsBuilder = array.NewStringBuilder(et.allocator)
		// Backfill nulls for previous rows
		for range rowIndex {
			et.errorBuilder.AppendNull()
			et.detailsBuilder.AppendNull()
		}
		et.hasErrors = true
	}
	et.errorBuilder.Append(types.SampleExtractionErrorType)
	et.detailsBuilder.Append(err.Error())
}

func (et *errorTracker) recordSuccess() {
	if et.hasErrors {
		et.errorBuilder.AppendNull()
		et.detailsBuilder.AppendNull()
	}
}

func (et *errorTracker) buildArrays() (arrow.Array, arrow.Array) {
	if !et.hasErrors {
		return nil, nil
	}
	return et.errorBuilder.NewArray(), et.detailsBuilder.NewArray()
}

func (et *errorTracker) releaseBuilders() {
	if et.hasErrors {
		et.errorBuilder.Release()
		et.detailsBuilder.Release()
	}
}

func ConvertFloat(v string) (float64, error) {
	return strconv.ParseFloat(v, 64)
}

func ConvertDuration(v string) (float64, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	return d.Seconds(), nil
}

func ConvertBytes(v string) (float64, error) {
	b, err := humanize.ParseBytes(v)
	if err != nil {
		return 0, err
	}
	return float64(b), nil
}
