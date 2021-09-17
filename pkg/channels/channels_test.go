package channels

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	addPipelineFunc = func(ctx context.Context, name string, input PipelineData) (output PipelineData, err error) {
		intAmount := input.Value.(int) + 1
		output.Value = intAmount
		return
	}

	multiplyPipelineFunc = func(ctx context.Context, name string, input PipelineData) (output PipelineData, err error) {
		intAmount := input.Value.(int) * 2
		output.Value = intAmount
		return
	}

	errorPipelineFunc = func(ctx context.Context, name string, input PipelineData) (PipelineData, error) {
		if input.Value.(int) == 10 {
			return PipelineData{}, errors.New("unexpected error")
		}

		return input, nil
	}
)

func TestCreatePipelineStep_Success(t *testing.T) {
	inputStream := GeneratorFromSlice(context.Background(), 1, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	addedStream := CreatePipelineStep(ctx, "add", inputStream, addPipelineFunc)
	multipliedStream := CreatePipelineStep(ctx, "multiply", addedStream, multiplyPipelineFunc)

	results := []interface{}{}
	for i := range OrDone(ctx, multipliedStream) {
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

	inputStream := GeneratorFromSlice(context.Background(), 1, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	addedStream := CreatePipelineStep(ctx, "add", inputStream, addPipelineFunc)
	multipliedStream := CreatePipelineStep(ctx, "multiply", addedStream, multiplyPipelineFunc)
	errorStream := CreatePipelineStep(ctx, "error", multipliedStream, errorPipelineFunc)

	results := []interface{}{}
	for i := range OrDone(ctx, errorStream) {
		if i.Err != nil {
			results = append(results, i.Err)
		} else {
			results = append(results, i.Value)
		}
	}

	expected := []interface{}{
		4, 6, 8, NewPipelineErr("error", errors.New("unexpected error")), 12, 14, 16, 18, 20,
	}

	require.Equal(t, expected, results)
}

func TestCreatePipelineStep_ErrorFirstStep(t *testing.T) {
	inputStream := GeneratorFromSlice(context.Background(), 10, 2, 3, 4, 5, 6, 7, 8, 9)

	ctx := context.Background()

	errorStream := CreatePipelineStep(ctx, "error", inputStream, errorPipelineFunc)
	addedStream := CreatePipelineStep(ctx, "add", errorStream, addPipelineFunc)
	multipliedStream := CreatePipelineStep(ctx, "multiply", addedStream, multiplyPipelineFunc)

	results := []interface{}{}
	for i := range OrDone(ctx, multipliedStream) {
		if i.Err != nil {
			results = append(results, i.Err)
		} else {
			results = append(results, i.Value)
		}
	}

	expected := []interface{}{
		NewPipelineErr("error", errors.New("unexpected error")), 6, 8, 10, 12, 14, 16, 18, 20,
	}

	require.Equal(t, expected, results)
}

func TestFanIn(t *testing.T) {
	channels := make([]<-chan PipelineData, 10)

	for i := 0; i < 10; i++ {
		a := i
		simpleChan := make(chan PipelineData)
		go func() {
			defer close(simpleChan)
			simpleChan <- PipelineData{Value: a}
		}()

		channels[i] = simpleChan
	}

	ctx := context.Background()
	output := FanIn(ctx, channels...)
	result := []int{}

	for elem := range output {
		result = append(result, elem.Value.(int))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, result)

}

func TestOrDone_CancelContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	<-time.After(time.Millisecond * 10)

	infiniteChannel := make(chan PipelineData)
	go func() {
		for {
			infiniteChannel <- PipelineData{Value: 1}
		}
	}()

	for result := range OrDone(ctx, infiniteChannel) {
		// do something with stream result here
		fmt.Println(result.Value)
	}

}

func TestOrDone_ClosedChannel(t *testing.T) {
	ctx := context.Background()

	channel := make(chan PipelineData)
	close(channel)

	result := <-OrDone(ctx, channel)
	require.Empty(t, result.Value)

}

func TestFanOut(t *testing.T) {
	ctx := context.Background()

	fanAmount := 10

	inputStream := make(chan PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount; i++ {
			j := i
			WriteOrDone(ctx, PipelineData{Value: j}, inputStream)
		}
	}()

	results := FanOut(ctx, inputStream, addPipelineFunc, fanAmount)

	r := FanIn(ctx, results...)

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
	inputStream := make(chan PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount*100; i++ {
			j := i
			expected = append(expected, j+1)
			WriteOrDone(ctx, PipelineData{Value: j}, inputStream)
		}
	}()

	results := FanOut(ctx, inputStream, addPipelineFunc, fanAmount)

	r := FanIn(ctx, results...)

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
	inputStream := make(chan PipelineData)
	go func() {
		defer close(inputStream)
		for i := 0; i < fanAmount/2; i++ {
			j := i
			expected = append(expected, j+1)
			WriteOrDone(ctx, PipelineData{Value: j}, inputStream)
		}
	}()

	results := FanOut(ctx, inputStream, addPipelineFunc, fanAmount)

	r := FanIn(ctx, results...)

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

	channel := make(chan PipelineData)
	go func() {
		defer close(channel)
		channel <- PipelineData{Value: 1}
	}()

	result := <-OrDone(ctx, channel)

	require.Equal(t, 1, result.Value)

}

func TestWriteOrDone(t *testing.T) {
	data := PipelineData{Value: 1}
	outputStream := make(chan PipelineData)
	go func() {
		WriteOrDone(context.Background(), data, outputStream)
	}()

	require.Equal(t, data, <-outputStream)
}

func TestWriteOrDone_CancelledContext(t *testing.T) {
	data := PipelineData{Value: 1}
	outputStream := make(chan PipelineData)

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)

	// this write deadlocks, because no one is listening to this outputStream
	WriteOrDone(ctx, data, outputStream)

	// so if tests reach this point, it's a success
	require.True(t, true)
}
