package streams_test

import (
	"context"
	"go-pipeline-stream/pkg/streams"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrDone_CancelContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	infiniteChannel := make(chan streams.PipelineData)

	select {
	case i := <-streams.OrDone(ctx, infiniteChannel):
		require.Equal(t, i.Err, context.DeadlineExceeded)
	case <-time.After(time.Second * 2):
		require.Fail(t, "timeout by the test mechanism, which means our OrDone check didn't function properly")
	}
}

func TestOrDone_ClosedChannel(t *testing.T) {
	ctx := context.Background()

	channel := make(chan streams.PipelineData)
	close(channel)

	result := <-streams.OrDone(ctx, channel)
	require.Empty(t, result.Value)
}

func TestOrDone_FinishReading(t *testing.T) {
	ctx := context.Background()

	v := &streamableInts{1}

	channel := make(chan streams.PipelineData)
	go func() {
		defer close(channel)
		channel <- streams.PipelineData{Value: v}
	}()

	result := <-streams.OrDone(ctx, channel)

	require.Equal(t, v, result.Value)
}

func TestWriteOrDone_CancelledContext(t *testing.T) {
	data := streams.PipelineData{Value: &streamableInts{1}}
	outputStream := make(chan streams.PipelineData)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	// this write deadlocks, because no one is listening to this OutputStream
	streams.WriteOrDone(ctx, data, outputStream)

	// so if tests reach this point, it's a success
	require.True(t, true)
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	builder := streams.NewPipelineBuilder("test")

	gen := intGenerator{length: 10}

	builder = builder.
		WithGenerator(gen.generateIntsFunc).
		WithSteps(slowFunc)

	p := builder.Build()

	result := p.Run(ctx)

	for e := range streams.OrDone(context.Background(), result) {
		require.Equal(t, context.DeadlineExceeded, e.Err)
	}
}

func TestNewTeardownStep_Validate_NegGoroutines_WillUseOne(t *testing.T) {
	step := streams.NewStep("test", -1,
		func(ctx context.Context, input streams.Streamable) (output []streams.Streamable, err error) {
			return []streams.Streamable{input}, nil
		})

	teardownStep := streams.NewTeardownStep("test", -1,
		func(ctx context.Context, input streams.Streamable) (err error) {
			return nil
		})

	gen := intGenerator{length: 100}

	p := streams.NewPipelineBuilder("test_negative_routines").
		WithGenerator(gen.generateIntsFunc).
		WithSteps(step).
		WithTeardownSteps(teardownStep).Build()

	resultStream := p.Run(context.Background())

	i := 1
	for e := range streams.OrDone(context.Background(), resultStream) {
		// validates if output slice is in the same provided order, which means everything ran with only 1 routine
		require.Equal(t, i, e.Value.(streamableInts).Integer)
		i++
	}
}
