package channels

import (
	"context"
	"sync"
)

// PipelineData is the struct we transit between our pipelineSteps.
// Ctx can be the context of the whole execution, or a ctx specific to this line (ex: ctx with lock tokens)
// Err should be checked in all steps, and if present, step should not do anything, just foward the error
// This patterns makes our error handling easier, caring about it only in the final steps of the pipeline
type PipelineData struct {
	Ctx   context.Context
	Value interface{}
	Err   error
}

func NewPipelineErr(err error) PipelineData {
	return PipelineData{Err: err}
}

type PipelineStepFunc func(ctx context.Context, input PipelineData) (output PipelineData, err error)

// GeneratorFromSlice creates a generator from an inputSlice
func GeneratorFromSlice(ctx context.Context, inputSlice ...interface{}) <-chan PipelineData {
	outputStream := make(chan PipelineData)
	go func() {
		defer close(outputStream)

		for _, v := range inputSlice {
			data := PipelineData{
				Ctx:   ctx,
				Value: v,
			}

			WriteOrDone(ctx, data, outputStream)
		}
	}()
	return outputStream
}

// CreatePipelineStep is a wrapper func for simple pipeline steps.
// It will generate a stream with the output of the createStepFunc, using the inputStream as input
func CreatePipelineStep(ctx context.Context,
	inputStream <-chan PipelineData,
	createStepFunc PipelineStepFunc) <-chan PipelineData {

	outputStream := make(chan PipelineData)
	go func() {
		defer close(outputStream)
		for i := range OrDone(ctx, inputStream) {
			// if err is not nil, we should do anything with this register, just forward it
			if i.Err != nil {
				WriteOrDone(ctx, i, outputStream)
			}

			output, err := createStepFunc(ctx, i)
			if err != nil {
				WriteOrDone(ctx, NewPipelineErr(err), outputStream)
			}

			WriteOrDone(ctx, output, outputStream)
		}
	}()

	return outputStream
}

// FanIn will join a variadic quantity of channels into a single channel
// It's useful after using the fan out pattern to spawn multiple goroutines to process the same input
// DO NOT use if the channels does not contain the same input, as it will lead to a output channel
// of different types.
func FanIn(ctx context.Context, channels ...<-chan PipelineData) <-chan PipelineData {
	wg := sync.WaitGroup{}

	multiplexedStream := make(chan PipelineData)
	multiplex := func(c <-chan PipelineData) {
		defer wg.Done()
		for element := range OrDone(ctx, c) {
			multiplexedStream <- element
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}

// WriteOrDone avoids deadlocking by ensuring context will be checked when performing a write
func WriteOrDone(ctx context.Context, write PipelineData, to chan<- PipelineData) {
	select {
	case <-ctx.Done():
	case to <- write:
	}
}

// OrDone will read encapsulate the logic of listening to the context's done or the input channel close
// It's useful to ensure goroutines will not be leaked and keep the code idiomatic.
func OrDone(ctx context.Context, c <-chan PipelineData) <-chan PipelineData {
	valStream := make(chan PipelineData)
	go func() {
		defer close(valStream)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if ok == false {
					return
				}
				select {
				case valStream <- v:
				case <-ctx.Done():
				}
			}
		}
	}()

	return valStream
}
