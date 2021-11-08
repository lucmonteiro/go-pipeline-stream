package streams

import (
	"context"
	"fmt"
	"sync"
)

type pipeline struct {
	name          string
	generatorFunc GeneratorFunc
	inputStream   Stream
	steps         []PipelineStep
	teardownSteps []TeardownStep
	errorHandler  ErrorHandlerFunc

	pipelineMetricName string
}

// Run executes the pipeline
func (p pipeline) Run(ctx context.Context) Stream {
	var outputStream Stream

	if p.inputStream != nil {
		outputStream = p.inputStream
	} else {
		inputStream := make(chan PipelineData)
		go func() {
			defer close(inputStream)
			p.generatorFunc(ctx, inputStream)
		}()

		outputStream = inputStream
	}

	// iterates through steps providing the outputStream
	// from the previous Step as input
	for _, step := range p.steps {
		outputStream = p.createStream(ctx, step, outputStream)
	}

	for _, teardownStep := range p.teardownSteps {
		outputStream = p.createTeardownStream(ctx, teardownStep, outputStream)
	}

	if p.errorHandler != nil {
		outputStream = p.createErrorHandlerStream(ctx, p.errorHandler, outputStream)
	}

	return outputStream
}

func (p pipeline) createStream(ctx context.Context, step PipelineStep, inputStream Stream) Stream {
	if step.goroutines > 1 {
		return p.fanOutFanIn(ctx, step, inputStream)
	}

	return p.createPipelineStream(ctx, step, inputStream)
}

// createPipelineStream is a wrapper func for simple pipeline steps.
// It will generate a streams with the output of the createStepFunc, using the inputStream as input.
func (p pipeline) createPipelineStream(ctx context.Context, step PipelineStep, inputStream Stream) Stream {
	outputStream := make(chan PipelineData)

	go func() {
		defer close(outputStream)

		for input := range OrDone(ctx, inputStream) {

			// if err is not nil, we shouldn't do anything with this register, just forward it
			if input.Err != nil {
				WriteOrDone(ctx, input, outputStream)
				continue
			}

			output, err := step.streamFunc(ctx, input.Value)
			if err != nil {
				input.Err = PipelineError{
					Step:     step.name,
					Cause:    err,
					StepType: StepTypePipelineStep,
				}

				WriteOrDone(ctx, input, outputStream)

				continue
			}

			for _, item := range output {
				WriteOrDone(ctx, PipelineData{Value: item}, outputStream)
			}

		}
	}()

	return outputStream
}

// createTeardownStream is a wrapper func for teardown steps.
// Teardown steps are used to close resources & etc and will always receive the PipelineData, regardless of a previous
// error in the pipeline. Commonly used to release locked resources.
// Errors returned by this func will be forwarded to the next Step - if one wants to ignore those errors,
// they can be checked through errors.Is(err, ErrTeardown)
func (p pipeline) createTeardownStream(ctx context.Context, step TeardownStep, inputStream Stream) Stream {
	outputStream := make(chan PipelineData)

	go func() {
		defer close(outputStream)

		for input := range OrDone(ctx, inputStream) {
			if err := step.teardownFunc(ctx, input.Value); err != nil {
				if input.Err == nil {
					input.Err = PipelineError{
						Step:     step.name,
						Cause:    fmt.Errorf("%w: %s", ErrTeardown, err),
						StepType: StepTypeTeardownStep,
					}
				}

				WriteOrDone(ctx, input, outputStream)
				continue
			}

			WriteOrDone(ctx, input, outputStream)
		}
	}()

	return outputStream
}

func (p pipeline) createErrorHandlerStream(ctx context.Context, handler ErrorHandlerFunc, inputStream Stream) Stream {
	outputStream := make(chan PipelineData)

	go func() {
		defer close(outputStream)

		for input := range OrDone(ctx, inputStream) {

			if input.Err == nil {
				WriteOrDone(ctx, input, outputStream)
				continue
			}

			if err := handler(ctx, input); err != nil {
				input.Err = PipelineError{
					Step:     _errorHandlerStepName,
					Cause:    err,
					StepType: StepTypeErrorHandlerStep,
				}

				WriteOrDone(ctx, input, outputStream)

				continue
			}

		}
	}()

	return outputStream
}

// fanOutFanIn utility func to expand and then join a Step.
func (p pipeline) fanOutFanIn(ctx context.Context, step PipelineStep, inputStream Stream) Stream {
	fannedOuts := p.fanOut(ctx, step, inputStream)
	return p.fanIn(ctx, fannedOuts...)
}

// fanOut will generate an array of streams given an input streams, processed by the pipeFunc
// Use fanIn after using this function in order to join the resulting streams into another streams.
func (p pipeline) fanOut(ctx context.Context, step PipelineStep, inputStream Stream) []Stream {
	streams := make([]Stream, step.goroutines)
	for i := 0; i < step.goroutines; i++ {
		streams[i] = p.createPipelineStream(ctx, step, inputStream)
	}

	// increase metric for fanned out with len of streams
	return streams
}

// fanIn will join a variadic quantity of streams into a single channel
// It's useful after using the fan out pattern to spawn multiple goroutines to process the same input .
func (p pipeline) fanIn(ctx context.Context, channels ...Stream) Stream {
	wg := sync.WaitGroup{} // used to know we processed all streams
	multiplexedStream := make(chan PipelineData)

	// create a func to write to the OutputStream given the input streams
	multiplexFunc := func(c Stream) {
		defer wg.Done()                       // tells wait group this channel is done
		for element := range OrDone(ctx, c) { // Range through input channel
			multiplexedStream <- element // write to output streams
		}
	}

	for _, c := range channels {
		wg.Add(1)           // tells wait group it has to wait for N streams
		go multiplexFunc(c) // start writing to the multiplexedStream
	}

	go func() { // start a async func that will close the multiplexedStream as soon as all streams are done reading from (or cancelled ctx)
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}
