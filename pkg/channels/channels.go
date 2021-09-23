package channels

import (
	"context"
	"fmt"
	"sync"
)

// PipelineErr custom err to carry the step where the error occurred name
type (
	PipelineErr struct {
		error
		Step string
	}

	// PipelineData is the struct we transit between our pipelineSteps.
	// Ctx can be the context of the whole execution, or a ctx specific to this line (ex: ctx with lock tokens)
	// Err should be checked in all steps, and if present, step should not do anything, just forward the error
	// This patterns makes our error handling easier, caring about it only in the final steps of the pipeline
	PipelineData struct {
		Value interface{}
		Err   error
	}

	// PipelineStepFunc is the func that we will use in our steps to transform data
	PipelineStepFunc func(ctx context.Context, input PipelineData) (output PipelineData, err error)

	// CreateStepRequest Wrapper struct to create pipeline steps
	CreateStepRequest struct {
		Name         string           // Name is the name of given step
		Func         PipelineStepFunc // Func is the func that this step will execute
		InputStream  Stream           // InputStream is the stream that a step will receive data from
		ReceiveError bool             // ReceiveError indicates if this step will receive the input even if previous steps err is not nil
	}

	Stream <-chan PipelineData
)

// NewPipelineErr wrapper for generating a pipeline error
func NewPipelineErr(step string, err error) error {
	return PipelineErr{
		error: err,
		Step:  step,
	}
}

// NewCreateStepRequest helper function to instantiate CreateStepRequest
func NewCreateStepRequest(name string, receiveError bool, inputStream <-chan PipelineData, stepFunc PipelineStepFunc) CreateStepRequest {
	return CreateStepRequest{
		Name:         name,
		Func:         stepFunc,
		InputStream:  inputStream,
		ReceiveError: receiveError,
	}
}

// GeneratorFromSlice creates a generator from an inputSlice
func GeneratorFromSlice(ctx context.Context, inputSlice ...interface{}) Stream {
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

// CreatePipelineStep is a wrapper func for simple pipeline steps.
// It will generate a stream with the output of the createStepFunc, using the inputStream as input.
func CreatePipelineStep(ctx context.Context, request CreateStepRequest) Stream {
	outputStream := make(chan PipelineData)
	go func() {
		defer close(outputStream)
		for input := range OrDone(ctx, request.InputStream) {
			// if err is not nil, we shouldn't do anything with this register, just forward it
			if input.Err != nil && !request.ReceiveError {
				WriteOrDone(ctx, input, outputStream)
				continue
			}

			output, err := request.Func(ctx, input)
			if err != nil {
				input.Err = err
				WriteOrDone(ctx, input, outputStream)
				continue
			}

			WriteOrDone(ctx, output, outputStream)

		}
	}()

	return outputStream
}

// FanOutFanIn utility func to expand and then join a step
func FanOutFanIn(ctx context.Context, fanAmount int, request CreateStepRequest) Stream {
	fannedOuts := FanOut(ctx, fanAmount, request)
	return FanIn(ctx, fannedOuts...)
}

// FanOut will generate an array of of streams given an input stream, processed by the pipeFunc
// Use FanIn after using this function in order to join the resulting streams into another stream.
func FanOut(ctx context.Context, fanAmount int, request CreateStepRequest) []Stream {
	finders := make([]Stream, fanAmount)
	for i := 0; i < fanAmount; i++ {
		request.Name = fmt.Sprintf("%s#%d", request.Name, i)
		finders[i] = CreatePipelineStep(ctx, request)
	}
	return finders
}

// FanIn will join a variadic quantity of channels into a single channel
// It's useful after using the fan out pattern to spawn multiple goroutines to process the same input .
func FanIn(ctx context.Context, channels ...Stream) Stream {
	wg := sync.WaitGroup{} // used to know we processed all channels
	multiplexedStream := make(chan PipelineData)

	// create a func to write to the OutputStream given the input channels
	multiplex := func(c Stream) {
		defer wg.Done()                       // tells wait group this channel is done
		for element := range OrDone(ctx, c) { //Range through input channel
			multiplexedStream <- element // write to output stream
		}
	}

	wg.Add(len(channels)) // tells wait group it has to wait for N channels
	for _, c := range channels {
		go multiplex(c) // start writing to the multiplexedStream
	}

	go func() { // start a async func that will close the multiplexedStream as soon as all channels are done reading from (or cancelled ctx)
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
func OrDone(ctx context.Context, inputStream <-chan PipelineData) Stream {
	outputStream := make(chan PipelineData)
	go func() {
		defer close(outputStream)
		for {
			select {
			case <-ctx.Done(): // exit as soon as the context is done
				return
			case v, ok := <-inputStream:
				if !ok { // ok == false means the input channel was closed
					return
				}
				select {
				case outputStream <- v: // write to value stream
				case <-ctx.Done(): //or exit if context is done and no one is reading from OutputStream (routine stuck in the write))
				}
			}
		}
	}()

	return outputStream
}
