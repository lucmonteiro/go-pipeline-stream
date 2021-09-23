package channels_test

import (
	"context"
	"errors"
	"go-pipeline-stream/pkg/channels"

	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	slowFunc = func(ctx context.Context, input channels.PipelineData) (output channels.PipelineData, err error) {
		<-time.After(time.Second * 10)
		return input, nil
	}

	addPipelineFunc = func(ctx context.Context, input channels.PipelineData) (output channels.PipelineData, err error) {
		intAmount := input.Value.(int) + 1
		output.Value = intAmount
		return
	}

	multiplyPipelineFunc = func(ctx context.Context, input channels.PipelineData) (output channels.PipelineData, err error) {
		intAmount := input.Value.(int) * 2
		output.Value = intAmount
		return
	}

	errorPipelineFunc = func(ctx context.Context, input channels.PipelineData) (channels.PipelineData, error) {
		if input.Value.(int) == 10 {
			return channels.PipelineData{}, errors.New("unexpected error")
		}

		return input, nil
	}

	handleErrFunc = func(ctx context.Context, input channels.PipelineData) (channels.PipelineData, error) {
		if input.Err != nil {
			return channels.PipelineData{Value: "error was handled gracefully"}, nil
		}

		return input, nil
	}
)

func TestCreatePipelineStep_Success(t *testing.T) {
	inputStream := channels.GeneratorFromSlice(context.Background(), 1, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	addedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("add", false, inputStream, addPipelineFunc))
	multipliedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("multiply", false, addedStream, multiplyPipelineFunc))

	results := []interface{}{}
	for i := range channels.OrDone(ctx, multipliedStream) {
		if i.Err != nil {
			results = append(results, i.Err)
		} else {
			results = append(results, i.Value)
		}
	}

	expected := []interface{}{
		4, 6, 8, 10, 12, 14, 16, 18, 20,
	}

	require.Equal(t, expected, results)
}

func TestCreatePipelineStep_Error_LastStep(t *testing.T) {

	inputStream := channels.GeneratorFromSlice(context.Background(), 1, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	addedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("add", false, inputStream, addPipelineFunc))
	multipliedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("multiply", false, addedStream, multiplyPipelineFunc))
	errorStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("error", false, multipliedStream, errorPipelineFunc))

	results := []interface{}{}
	for i := range channels.OrDone(ctx, errorStream) {
		if i.Err != nil {
			results = append(results, i.Err)
		} else {
			results = append(results, i.Value)
		}
	}

	expected := []interface{}{
		4, 6, 8, errors.New("unexpected error"), 12, 14, 16, 18, 20,
	}

	require.Equal(t, expected, results)
}

func TestCreatePipelineStep_ErrorFirstStep(t *testing.T) {
	inputStream := channels.GeneratorFromSlice(context.Background(), 10, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	errorStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("error", false, inputStream, errorPipelineFunc))
	addedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("add", false, errorStream, addPipelineFunc))
	multipliedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("multiply", false, addedStream, multiplyPipelineFunc))

	results := []interface{}{}
	for i := range channels.OrDone(ctx, multipliedStream) {
		if i.Err != nil {
			results = append(results, i.Err)
		} else {
			results = append(results, i.Value)
		}
	}

	expected := []interface{}{
		errors.New("unexpected error"), 6, 8, 10, 12, 14, 16, 18, 20,
	}

	require.Equal(t, expected, results)
}

func TestCreatePipelineStep_ErrorFirstStep_LastStepHandleErr(t *testing.T) {
	inputStream := channels.GeneratorFromSlice(context.Background(), 10, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	errorStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("error", false, inputStream, errorPipelineFunc))
	addedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("add", false, errorStream, addPipelineFunc))
	multipliedStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("multiply", false, addedStream, multiplyPipelineFunc))
	handleErrorStream := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("handleErr", true, multipliedStream, handleErrFunc))

	results := []interface{}{}
	for i := range channels.OrDone(ctx, handleErrorStream) {
		if i.Err != nil {
			results = append(results, i.Err)
		} else {
			results = append(results, i.Value)
		}
	}

	expected := []interface{}{
		"error was handled gracefully", 6, 8, 10, 12, 14, 16, 18, 20,
	}

	require.Equal(t, expected, results)
}

func TestFanIn(t *testing.T) {
	chans := make([]channels.Stream, 10)

	for i := 0; i < 10; i++ {
		a := i
		simpleChan := make(chan channels.PipelineData)
		go func() {
			defer close(simpleChan)
			simpleChan <- channels.PipelineData{Value: a}
		}()

		chans[i] = simpleChan
	}

	ctx := context.Background()
	output := channels.FanIn(ctx, chans...)
	result := []int{}

	for elem := range output {
		result = append(result, elem.Value.(int))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, result)

}

func TestFanOutFanIn(t *testing.T) {
	ctx := context.Background()

	fanAmount := 10

	inputStream := make(chan channels.PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount; i++ {
			j := i
			channels.WriteOrDone(ctx, channels.PipelineData{Value: j}, inputStream)
		}
	}()

	results := channels.FanOutFanIn(ctx, fanAmount, channels.NewCreateStepRequest("add", false, inputStream, addPipelineFunc))

	resultSlice := []int{}
	for data := range results {
		if data.Value != nil {
			resultSlice = append(resultSlice, data.Value.(int))
		}
	}

	sort.Slice(resultSlice, func(i, j int) bool {
		return resultSlice[i] < resultSlice[j]
	})

	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, resultSlice)
}

func TestOrDone_CancelContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	infiniteChannel := make(chan channels.PipelineData)

	select {
	case <-channels.OrDone(ctx, infiniteChannel):
		// reaching this part of code means test passed
		require.True(t, true)
	case <-time.After(time.Second * 2):
		require.Fail(t, "timeout by the test mechanism, which means our OrDone check didn't function properly")
	}

}

func TestOrDone_TimeoutOnStep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	gen := channels.GeneratorFromSlice(ctx, 1, 2, 3, 4)
	s := channels.CreatePipelineStep(ctx, channels.NewCreateStepRequest("slowmo", false, gen, slowFunc))

	select {
	case <-channels.OrDone(ctx, s):
		// reaching this part of code means test passed
		require.True(t, true)
	case <-time.After(time.Second * 2):
		require.Fail(t, "timeout by the test mechanism, which means our OrDone check didn't function properly")
	}

}

func TestOrDone_ClosedChannel(t *testing.T) {
	ctx := context.Background()

	channel := make(chan channels.PipelineData)
	close(channel)

	result := <-channels.OrDone(ctx, channel)
	require.Empty(t, result.Value)

}

func TestFanOut(t *testing.T) {
	ctx := context.Background()

	fanAmount := 10

	inputStream := make(chan channels.PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount; i++ {
			j := i
			channels.WriteOrDone(ctx, channels.PipelineData{Value: j}, inputStream)
		}
	}()

	results := channels.FanOut(ctx, fanAmount, channels.NewCreateStepRequest("add", false, inputStream, addPipelineFunc))

	r := channels.FanIn(ctx, results...)

	resultSlice := []int{}
	for data := range r {
		if data.Value != nil {
			resultSlice = append(resultSlice, data.Value.(int))
		}
	}

	sort.Slice(resultSlice, func(i, j int) bool {
		return resultSlice[i] < resultSlice[j]
	})

	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, resultSlice)
}

func TestFanOut_MoreValuesThanChannels(t *testing.T) {
	ctx := context.Background()

	fanAmount := 10

	expected := []int{}
	inputStream := make(chan channels.PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount*100; i++ {
			j := i
			expected = append(expected, j+1)
			channels.WriteOrDone(ctx, channels.PipelineData{Value: j}, inputStream)
		}
	}()

	results := channels.FanOut(ctx, fanAmount, channels.NewCreateStepRequest("add", false, inputStream, addPipelineFunc))

	r := channels.FanIn(ctx, results...)

	resultSlice := []int{}
	for data := range r {
		if data.Value != nil {
			resultSlice = append(resultSlice, data.Value.(int))
		}
	}

	sort.Slice(resultSlice, func(i, j int) bool {
		return resultSlice[i] < resultSlice[j]
	})

	require.Equal(t, expected, resultSlice)

}

func TestFanOut_LessValuesThanChannels(t *testing.T) {
	ctx := context.Background()

	fanAmount := 10

	expected := []int{}
	inputStream := make(chan channels.PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount/2; i++ {
			j := i
			expected = append(expected, j+1)
			channels.WriteOrDone(ctx, channels.PipelineData{Value: j}, inputStream)
		}
	}()

	results := channels.FanOut(ctx, fanAmount, channels.NewCreateStepRequest("add", false, inputStream, addPipelineFunc))

	r := channels.FanIn(ctx, results...)

	resultSlice := []int{}
	for data := range r {
		if data.Value != nil {
			resultSlice = append(resultSlice, data.Value.(int))
		}
	}

	sort.Slice(resultSlice, func(i, j int) bool {
		return resultSlice[i] < resultSlice[j]
	})

	require.Equal(t, expected, resultSlice)

}

func TestOrDone_FinishReading(t *testing.T) {

	ctx := context.Background()

	channel := make(chan channels.PipelineData)
	go func() {
		defer close(channel)
		channel <- channels.PipelineData{Value: 1}
	}()

	result := <-channels.OrDone(ctx, channel)

	require.Equal(t, 1, result.Value)
}

func TestWriteOrDone(t *testing.T) {
	data := channels.PipelineData{Value: 1}
	outputStream := make(chan channels.PipelineData)
	go func() {
		channels.WriteOrDone(context.Background(), data, outputStream)
	}()

	require.Equal(t, data, <-outputStream)
}

func TestWriteOrDone_CancelledContext(t *testing.T) {
	data := channels.PipelineData{Value: 1}
	outputStream := make(chan channels.PipelineData)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	// this write deadlocks, because no one is listening to this OutputStream
	channels.WriteOrDone(ctx, data, outputStream)

	// so if tests reach this point, it's a success
	require.True(t, true)
}
