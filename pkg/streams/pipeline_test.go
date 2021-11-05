package streams_test

import (
	"context"
	"errors"
	"fmt"
	"go-pipeline-stream/pkg/streams"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	addStep         = streams.NewStep("add_step", 10, addStepFunc)
	doubleStep      = streams.NewStep("multiply_step", 10, doubleStepFunc)
	cloneStep       = streams.NewStep("clone", 10, cloneStepFunc)
	generateErrStep = streams.NewStep("err", 10, errStepFunc)
	slowFunc        = streams.NewStep("slow", 10, slowStepFunc)

	// errorHandlerThatErrors will always return error if triggered, used to test scenarios where shouldn't
	// exist and error, or error in error handler.
	errorHandlerThatErrors = func(ctx context.Context, input streams.PipelineData) (err error) {
		return errors.New("unexpected error")
	}
)

type streamableInts struct {
	Integer int
}

func (s streamableInts) Stream() {}

type teardownMock struct {
	mock.Mock
}

func (t *teardownMock) teardownFunc(_ context.Context, input streams.Streamable) (err error) {
	args := t.Called(input)
	return args.Error(0)
}

type errorHandlerMock struct {
	mock.Mock
}

func (e *errorHandlerMock) handleErrorFunc(_ context.Context, input streams.PipelineData) (err error) {
	args := e.Called(input)
	return args.Error(0)
}

type intGenerator struct {
	streams.Streamable
	length int
}

func (g intGenerator) generateIntsFunc(ctx context.Context, stream chan streams.PipelineData) {
	for i := 1; i <= g.length; i++ {
		streams.WriteOrDone(ctx, streams.PipelineData{Value: streamableInts{Integer: i}}, stream)
	}
}

func addStepFunc(_ context.Context, input streams.Streamable) (output []streams.Streamable, err error) {
	s := input.(streamableInts)

	s.Integer = s.Integer + 1
	return []streams.Streamable{s}, nil
}

func slowStepFunc(_ context.Context, input streams.Streamable) (output []streams.Streamable, err error) {
	<-time.After(time.Second * 10)
	return []streams.Streamable{input}, nil
}

func doubleStepFunc(_ context.Context, input streams.Streamable) (output []streams.Streamable, err error) {
	s := input.(streamableInts)

	s.Integer = s.Integer * 2
	return []streams.Streamable{s}, nil
}

func errStepFunc(_ context.Context, input streams.Streamable) (output []streams.Streamable, err error) {
	s := input.(streamableInts)

	if s.Integer%2 == 0 {
		return nil, errors.New("unexpected")
	}

	return []streams.Streamable{s}, nil
}

// cloneStepFunc clones an input in order to validate the []Streamble return
func cloneStepFunc(_ context.Context, input streams.Streamable) (output []streams.Streamable, err error) {
	return []streams.Streamable{input, input}, nil
}

// all success with error handler
func TestPipelineBuilder_WithErrorHandler_AllSuccess(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(doubleStep, addStep).
		WithErrorHandler(errorHandlerThatErrors)

	expectedValues := []int{3, 5, 7, 9, 11, 13, 15, 17, 19, 21}

	p := builder.Build()

	result := p.Run(context.Background())
	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))
}

func TestPipelineBuilder_WithoutGracefulErrorHandler_ErrorInFirstStep(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	gen := intGenerator{length: 2}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep, addStep, doubleStep)

	p := builder.Build()

	result := p.Run(context.Background())

	actual := []streams.PipelineData{}

	expected := []streams.PipelineData{
		{Value: streamableInts{Integer: 2}, Err: streams.PipelineError{
			Step:     "err",
			Cause:    errors.New("unexpected"),
			StepType: "pipeline",
		}},
		{Value: streamableInts{Integer: 4}},
	}

	for r := range streams.OrDone(context.Background(), result) {
		actual = append(actual, r)
	}

	sortPipelineDataSlice(actual)

	require.Equal(t, expected, actual)
}

func TestPipelineBuilder_WithoutGracefulErrorHandler(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	gen := intGenerator{length: 3}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep)

	p := builder.Build()
	result := p.Run(context.Background())

	expected := []streams.PipelineData{
		{Value: streamableInts{Integer: 1}},
		{
			Value: streamableInts{Integer: 2},
			Err: streams.PipelineError{
				Step:     "err",
				Cause:    errors.New("unexpected"),
				StepType: "pipeline",
			},
		},
		{Value: streamableInts{Integer: 3}},
	}

	actual := []streams.PipelineData{}

	for r := range streams.OrDone(context.Background(), result) {
		actual = append(actual, r)
	}

	// sorting the error to the end of the slice
	sortPipelineDataSlice(actual)

	require.Equal(t, expected, actual)
}

func TestPipelineBuilder_WithGracefulErrorHandler(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	mockedErr := &errorHandlerMock{}

	gen := intGenerator{length: 2}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep).
		WithErrorHandler(mockedErr.handleErrorFunc)

	errData := streams.PipelineData{Value: streamableInts{Integer: 2}, Err: streams.PipelineError{
		Step:     "err",
		Cause:    errors.New("unexpected"),
		StepType: "pipeline",
	}}
	mockedErr.On("handleErrorFunc", errData).Return(nil)

	p := builder.Build()
	result := p.Run(context.Background())

	for e := range streams.OrDone(context.Background(), result) {
		require.Nil(t, e.Err)
	}

	require.True(t, mockedErr.AssertExpectations(t))
}

func TestPipelineBuilder_WithTeardown_WithGracefulErrorHandler(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	mockedErr := &errorHandlerMock{}
	mockedTeardown := &teardownMock{}

	gen := intGenerator{length: 2}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep).
		WithTeardownSteps(streams.NewTeardownStep("mock", 1, mockedTeardown.teardownFunc)).
		WithErrorHandler(mockedErr.handleErrorFunc)

	successData := streams.PipelineData{Value: streamableInts{Integer: 1}}
	mockedTeardown.On("teardownFunc", successData.Value).Return(nil)

	errData := streams.PipelineData{Value: streamableInts{Integer: 2}, Err: streams.PipelineError{
		Step:     "err",
		Cause:    errors.New("unexpected"),
		StepType: "pipeline",
	}}
	mockedTeardown.On("teardownFunc", errData.Value).Return(nil)
	mockedErr.On("handleErrorFunc", errData).Return(nil)

	p := builder.Build()

	result := p.Run(context.Background())

	for e := range streams.OrDone(context.Background(), result) {
		require.Nil(t, e.Err)
	}

	require.True(t, mockedTeardown.AssertExpectations(t))
	require.True(t, mockedErr.AssertExpectations(t))
}

func TestPipelineBuilder_WithTeardownError_WithGracefulErrorHandler(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	mockedErr := &errorHandlerMock{}
	mockedTeardown := &teardownMock{}

	gen := intGenerator{length: 1}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep).
		WithTeardownSteps(streams.NewTeardownStep("mock", 1, mockedTeardown.teardownFunc)).
		WithErrorHandler(mockedErr.handleErrorFunc)

	successData := streams.PipelineData{Value: streamableInts{Integer: 1}}
	mockedTeardown.On("teardownFunc", successData.Value).Return(errors.New("unexpected"))

	dataAfterTeardownErr := streams.PipelineData{Value: streamableInts{Integer: 1}, Err: streams.PipelineError{
		Step:     "mock",
		Cause:    fmt.Errorf("%w: %s", streams.ErrTeardown, errors.New("unexpected")),
		StepType: streams.StepTypeTeardownStep,
	}}
	mockedErr.On("handleErrorFunc", dataAfterTeardownErr).Return(nil)

	p := builder.Build()

	result := p.Run(context.Background())

	for e := range streams.OrDone(context.Background(), result) {
		require.Nil(t, e.Err)
	}

	require.True(t, mockedTeardown.AssertExpectations(t))
	require.True(t, mockedErr.AssertExpectations(t))
}

func TestPipelineBuilder_WithTeardownError_Comparable(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")
	mockedTeardown := &teardownMock{}

	gen := intGenerator{length: 1}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep).
		WithTeardownSteps(streams.NewTeardownStep("mock", 1, mockedTeardown.teardownFunc))

	successData := streams.PipelineData{Value: streamableInts{Integer: 1}}
	mockedTeardown.On("teardownFunc", successData.Value).Return(errors.New("unexpected"))

	p := builder.Build()

	result := p.Run(context.Background())

	for e := range streams.OrDone(context.Background(), result) {
		require.True(t, errors.Is(e.Err, streams.ErrTeardown))
	}

	require.True(t, mockedTeardown.AssertExpectations(t))
}

func TestPipelineBuilder_WithErrorHandler_ThatFails(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests")

	mockedErr := &errorHandlerMock{}

	gen := intGenerator{length: 4}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(generateErrStep).
		WithErrorHandler(mockedErr.handleErrorFunc)

	errData := streams.PipelineData{Value: streamableInts{Integer: 2}, Err: streams.PipelineError{
		Step:     "err",
		Cause:    errors.New("unexpected"),
		StepType: streams.StepTypePipelineStep,
	}}
	mockedErr.On("handleErrorFunc", errData).Return(errors.New("unexpected error in error handler"))

	errData = streams.PipelineData{Value: streamableInts{Integer: 4}, Err: streams.PipelineError{
		Step:     "err",
		Cause:    errors.New("unexpected"),
		StepType: "pipeline",
	}}
	mockedErr.On("handleErrorFunc", errData).Return(nil)

	p := builder.Build()

	result := p.Run(context.Background())

	unhandledErrQuantity := 0

	for e := range streams.OrDone(context.Background(), result) {
		if e.Err != nil {
			require.Equal(t, streams.PipelineError{
				Step:     "error_handler",
				Cause:    errors.New("unexpected error in error handler"),
				StepType: streams.StepTypeErrorHandlerStep,
			}, e.Err)

			unhandledErrQuantity++
		}
	}

	require.Equal(t, 1, unhandledErrQuantity)
	require.True(t, mockedErr.AssertExpectations(t))
}

func TestPipeline_Run_Without_Goroutines(t *testing.T) {
	builder := streams.NewPipelineBuilder("test")
	addStepNoRoutines := streams.NewStep("add_step", 1, addStepFunc)

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(addStepNoRoutines, addStep, addStep)

	expectedValues := []int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

	p := builder.Build()

	result := p.Run(context.Background())
	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))
}

func TestPipeline_Run_With_BadGoroutinesParam(t *testing.T) {
	builder := streams.NewPipelineBuilder("test")
	addStepNoRoutines := streams.NewStep("add_step", -1, addStepFunc)

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(addStepNoRoutines, addStep, addStep)

	expectedValues := []int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

	p := builder.Build()

	result := p.Run(context.Background())
	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))
}

func TestPipeline_Run_With_Goroutines(t *testing.T) {
	builder := streams.NewPipelineBuilder("test")

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(addStep, addStep, addStep)

	expectedValues := []int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

	p := builder.Build()

	result := p.Run(context.Background())

	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))
}

func TestOneToManyStep(t *testing.T) {
	builder := streams.NewPipelineBuilder("test")

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(cloneStep, addStep, addStep, addStep)

	expectedValues := []int{4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13}

	p := builder.Build()

	result := p.Run(context.Background())

	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))
}

func TestOrderOfTheSteps(t *testing.T) {
	builder := streams.NewPipelineBuilder("test")

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(doubleStep, addStep)

	expectedValues := []int{3, 5, 7, 9, 11, 13, 15, 17, 19, 21}

	p := builder.Build()
	result := p.Run(context.Background())

	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))

	builder = streams.NewPipelineBuilder("test")

	gen = intGenerator{length: 10}
	builder = builder.WithGenerator(gen.generateIntsFunc).
		WithSteps(addStep, doubleStep)

	expectedValues = []int{4, 6, 8, 10, 12, 14, 16, 18, 20, 22}

	p = builder.Build()
	result = p.Run(context.Background())

	require.Equal(t, expectedValues, extractIntArrayFromResultStream(t, result))
}

func extractIntArrayFromResultStream(t *testing.T, result streams.Stream) []int {
	actualValues := []int{}

	for r := range streams.OrDone(context.Background(), result) {
		require.Nil(t, r.Err)
		actualValues = append(actualValues, r.Value.(streamableInts).Integer)
	}

	sort.Ints(actualValues)
	return actualValues
}

func sortPipelineDataSlice(actual []streams.PipelineData) {
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Value.(streamableInts).Integer < actual[j].Value.(streamableInts).Integer
	})
}
