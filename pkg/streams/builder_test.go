package streams_test

import (
	"github.com/stretchr/testify/require"
	"go-pipeline-stream/pkg/streams"
	"testing"
)

func TestPipelineBuilder_NoGeneratorAndNoInputStream(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests").WithSteps(addStep)

	require.Panics(t, func() {
		p := builder.Build()
		require.Empty(t, p)
	})
}

func TestPipelineBuilder_NoGenerator(t *testing.T) {
	builder := streams.NewPipelineBuilder("tests").WithSteps(addStep).
		WithInputStream(make(chan streams.PipelineData))

	require.NotPanics(t, func() {
		p := builder.Build()
		require.NotEmpty(t, p)
	})
}

func TestPipelineBuilder_NoInputStream(t *testing.T) {
	i := intGenerator{}

	builder := streams.NewPipelineBuilder("tests").WithSteps(addStep).
		WithGenerator(i.generateIntsFunc)

	require.NotPanics(t, func() {
		p := builder.Build()
		require.NotEmpty(t, p)
	})
}

func TestPipelineBuilder_NoSteps(t *testing.T) {
	g := intGenerator{}
	builder := streams.NewPipelineBuilder("tests").WithGenerator(g.generateIntsFunc)

	require.Panics(t, func() {
		p := builder.Build()
		require.Empty(t, p)
	})
}
