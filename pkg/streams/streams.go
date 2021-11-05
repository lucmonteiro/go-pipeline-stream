package streams

import (
	"context"
	"errors"
	"time"
)

var (
	ErrTeardown = errors.New("error executing teardown step")
)

const (
	StepTypePipelineStep     StepType = "pipeline"
	StepTypeTeardownStep     StepType = "teardown"
	StepTypeErrorHandlerStep StepType = "error_handler"

	_errorHandlerStepName = "error_handler"
)

type (
	// Stream is a receive-only channel of PipelineData
	// Using receive-only channels is good to avoid someone writing in a wrong channel.
	Stream <-chan PipelineData

	// Streamable interface that must be embedded in order to Stream in the pipeline data
	Streamable interface {
		Stream()
	}

	Pipeline interface {
		Run(ctx context.Context) Stream
	}

	// PipelineData is the struct we transit between our pipelineSteps.
	// Err should be checked in all steps, and if present, Step should not do anything, just forward the error
	// This patterns makes our error handling easier, caring about it only in the final steps of the pipeline.
	PipelineData struct {
		Value Streamable
		Err   error
	}

	// GeneratorFunc is the func that will be executed first by the pipeline, it must write the input data in the
	// inputStream using WriteOrDone func
	// errors that happen in this func should also be written in the Stream (using the Err field)
	GeneratorFunc func(ctx context.Context, inputStream chan PipelineData)

	// StreamFunc is the func that we will use in our steps to transform data.
	// Output is an []Streamable since in some scenarios one input can generate N outputs to be processed
	// for example, a confirmed batch of transactions is 1-N.
	StreamFunc func(ctx context.Context, input Streamable) (output []Streamable, err error)

	// TeardownFunc functions to close opened resources and release locks.
	// Will always execute at the end of the pipeline,
	TeardownFunc func(ctx context.Context, input Streamable) (err error)

	// ErrorHandlerFunc function to gracefully handle errors in the pipeline
	ErrorHandlerFunc func(ctx context.Context, input PipelineData) (err error)

	// PipelineStep represents a Step of the pipeline that will be executed
	// name is the Step's name that will be tagged in the pipeline metric
	// goroutines is used to control the number of concurrent goroutines running the same Step. If < 1, will be 1.
	PipelineStep struct {
		name       string
		streamFunc StreamFunc
		goroutines int
	}

	// TeardownStep is a step that is always executed (regardless of previous errors).
	// It's used to free resources / release locks
	// Errors will be forwarded to the ErrorHandler, they can be checked through comparing with the ErrTeardown err.
	TeardownStep struct {
		name         string
		teardownFunc TeardownFunc
		goroutines   int
	}

	PipelineError struct {
		Step     string
		Cause    error
		StepType StepType
	}

	StepType string
)

func (p PipelineError) Error() string {
	return "error executing pipeline: " + p.Cause.Error()
}

func (p PipelineError) Unwrap() error {
	return p.Cause
}

// NewStep creates a PipelineStep to be added to a PipelineBuilder
func NewStep(name string, goroutines int, streamFunc StreamFunc) PipelineStep {
	if goroutines < 1 {
		goroutines = 1
	}

	return PipelineStep{
		name:       name,
		streamFunc: streamFunc,
		goroutines: goroutines,
	}
}

// NewTeardownStep creates a TeardownStep to be added to a PipelineBuilder
func NewTeardownStep(name string, goroutines int, teardownFunc TeardownFunc) TeardownStep {
	if goroutines < 1 {
		goroutines = 1
	}

	return TeardownStep{
		name:         name,
		teardownFunc: teardownFunc,
		goroutines:   goroutines,
	}
}

// WriteOrDone avoids deadlocking by ensuring context will be checked when performing a write.
func WriteOrDone(ctx context.Context, write PipelineData, to chan<- PipelineData) {
	select {
	case <-ctx.Done():
	case to <- write:
	}
}

// OrDone will read encapsulate the logic of listening to the context's done or the input channel close
// It's useful to ensure goroutines will not be leaked and keep the code idiomatic.
func OrDone(ctx context.Context, inputStream Stream) Stream {
	outputStream := make(chan PipelineData)
	go func() {
		defer close(outputStream)
		for {
			select {
			case <-ctx.Done(): // exit as soon as the context is done
				select {
				case outputStream <- PipelineData{Err: ctx.Err()}:
					return
				case <-time.After(time.Second): // safeguard in case no routines are listening to the outputStream
					return
				}
			case v, ok := <-inputStream:
				if !ok { // ok == false means the input channel was closed
					return
				}

				WriteOrDone(ctx, v, outputStream)
			}
		}
	}()

	return outputStream
}

// GenerateFromSlice generates a stream from an inputSlice.
func GenerateFromSlice(ctx context.Context, inputSlice []Streamable) Stream {
	outputStream := make(chan PipelineData)
	go func() {
		defer close(outputStream)

		for _, v := range inputSlice {
			data := PipelineData{
				Value: v,
			}

			WriteOrDone(ctx, data, outputStream)
		}
	}()
	return outputStream
}
