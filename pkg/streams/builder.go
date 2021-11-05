package streams

import (
	"fmt"
)

// PipelineBuilder prepares a pipeline for execution
// pipeline will always output all register with errors.
type PipelineBuilder struct {
	name string

	generatorFunc GeneratorFunc
	inputStream   Stream

	steps         []PipelineStep
	teardownSteps []TeardownStep
	errorHandler  ErrorHandlerFunc
}

// NewPipelineBuilder creates a new builder with given name and a metric name initialized
func NewPipelineBuilder(name string) *PipelineBuilder {
	return &PipelineBuilder{
		name: name,
	}
}

// WithGenerator should be called in order to provide the pipeline a generator function (an input)
func (p *PipelineBuilder) WithGenerator(f GeneratorFunc) *PipelineBuilder {
	p.generatorFunc = f
	return p
}

func (p *PipelineBuilder) WithInputStream(inputStream Stream) *PipelineBuilder {
	p.inputStream = inputStream
	return p
}

// WithSteps should be called 1...N times to provide the PipelineBuilder the steps to execute
func (p *PipelineBuilder) WithSteps(steps ...PipelineStep) *PipelineBuilder {
	p.steps = append(p.steps, steps...)
	return p
}

// WithTeardownSteps teardown steps to release resources
func (p *PipelineBuilder) WithTeardownSteps(teardownSteps ...TeardownStep) *PipelineBuilder {
	p.teardownSteps = append(p.teardownSteps, teardownSteps...)
	return p
}

// WithErrorHandler Step to handle errors
func (p *PipelineBuilder) WithErrorHandler(errorHandlerFunc ErrorHandlerFunc) *PipelineBuilder {
	p.errorHandler = errorHandlerFunc
	return p
}

func (p PipelineBuilder) Build() Pipeline {
	if err := p.validatePipeline(); err != nil {
		panic("pipeline is invalid and MUST compile")
	}

	return &pipeline{
		name:          p.name,
		generatorFunc: p.generatorFunc,
		inputStream:   p.inputStream,
		steps:         p.steps,
		teardownSteps: p.teardownSteps,
		errorHandler:  p.errorHandler,
	}
}

func (p PipelineBuilder) validatePipeline() error {
	if p.generatorFunc == nil && p.inputStream == nil {
		return fmt.Errorf("error validating pipeline: generatorFunc and inputStream is empty, must provide one")
	}

	if p.generatorFunc != nil && p.inputStream != nil {
		return fmt.Errorf("error validating pipeline: generatorFunc and inputStream provided, must provide only one")
	}

	if len(p.steps) < 1 {
		return fmt.Errorf("error validating pipeline: no steps for pipeline")
	}
	return nil
}
