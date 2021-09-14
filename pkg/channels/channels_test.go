package channels

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCreatePipelineStep(t *testing.T) {

	inputStream := GeneratorFromSlice(context.Background(), 1, 2, 3, 4, 5, 6, 7, 8, 9)
	addStep := func(ctx context.Context, input PipelineData) (output PipelineData, err error) {
		intAmount := input.Value.(int) + 1
		output.Value = intAmount
		return
	}

	multiplyStep := func(ctx context.Context, input PipelineData) (output PipelineData, err error) {
		intAmount := input.Value.(int) * 2
		output.Value = intAmount
		return
	}

	ctx := context.Background()

	addedStream := CreatePipelineStep(ctx, inputStream, addStep)
	multipliedStream := CreatePipelineStep(ctx, addedStream, multiplyStep)

	results := []int{}

	for i := range OrDone(ctx, multipliedStream) {
		results = append(results, i.Value.(int))
	}

	expected := []int{
		4, 6, 8, 10, 12, 14, 16, 18, 20,
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	infiniteChannel := make(chan PipelineData)
	go func() {
		for {
			infiniteChannel <- PipelineData{Value: 1}
		}
	}()

	done := OrDone(ctx, infiniteChannel)

	require.NotEmpty(t, <-done)
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
