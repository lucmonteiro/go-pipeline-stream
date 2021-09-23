package streams

import (
	"context"
	"fmt"
	"sync"
)

type (

	// PipelineData is the struct we transit between our pipelineSteps.
	// Ctx can be the context of the whole execution, or a ctx specific to this line (ex: ctx with lock tokens)
	// Err should be checked in all steps, and if present, step should not do anything, just forward the error
	// This patterns makes our error handling easier, caring about it only in the final steps of the pipeline
	PipelineData struct {
		Value interface{}
		Err   error
	}

	// StreamFunc is the func that we will use in our steps to transform data
	StreamFunc func(ctx context.Context, input PipelineData) (output PipelineData, err error)

	// CreateStreamRequest Wrapper struct to create pipeline steps
	CreateStreamRequest struct {
		Name         string     // Name is the name of given step
		Func         StreamFunc // Func is the func that this step will execute
		InputStream  Stream     // InputStream is the streams that a step will receive data from
		ReceiveError bool       // ReceiveError indicates if this step will receive the input even if previous steps err is not nil
	}

	Stream <-chan PipelineData
)

// NewCreateStreamRequest helper function to instantiate CreateStreamRequest
func NewCreateStreamRequest(name string, receiveError bool, inputStream <-chan PipelineData, stepFunc StreamFunc) CreateStreamRequest {
	return CreateStreamRequest{
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

// CreatePipelineStream is a wrapper func for simple pipeline steps.
// It will generate a streams with the output of the createStepFunc, using the inputStream as input.
func CreatePipelineStream(ctx context.Context, request CreateStreamRequest) Stream {
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
func FanOutFanIn(ctx context.Context, fanAmount int, request CreateStreamRequest) Stream {
	fannedOuts := FanOut(ctx, fanAmount, request)
	return FanIn(ctx, fannedOuts...)
}

// FanOut will generate an array of of streams given an input streams, processed by the pipeFunc
// Use FanIn after using this function in order to join the resulting streams into another streams.
func FanOut(ctx context.Context, fanAmount int, request CreateStreamRequest) []Stream {
	streams := make([]Stream, fanAmount)
	for i := 0; i < fanAmount; i++ {
		request.Name = fmt.Sprintf("%s#%d", request.Name, i)
		streams[i] = CreatePipelineStream(ctx, request)
	}

	// increase metric for fanned out with len of streams
	return streams
}

// FanIn will join a variadic quantity of streams into a single channel
// It's useful after using the fan out pattern to spawn multiple goroutines to process the same input .
func FanIn(ctx context.Context, channels ...Stream) Stream {
	wg := sync.WaitGroup{} // used to know we processed all streams
	multiplexedStream := make(chan PipelineData)

	// create a func to write to the OutputStream given the input streams
	multiplex := func(c Stream) {
		defer wg.Done()                       // tells wait group this channel is done
		for element := range OrDone(ctx, c) { //Range through input channel
			multiplexedStream <- element // write to output streams
		}
		// increase
	}

	wg.Add(len(channels)) // tells wait group it has to wait for N streams
	for _, c := range channels {
		go multiplex(c) // start writing to the multiplexedStream
	}

	go func() { // start a async func that will close the multiplexedStream as soon as all streams are done reading from (or cancelled ctx)
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
				case outputStream <- v: // write to value streams
				case <-ctx.Done(): //or exit if context is done and no one is reading from OutputStream (routine stuck in the write))
				}
			}
		}
	}()

	return outputStream
}
