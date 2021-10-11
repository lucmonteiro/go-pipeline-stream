package withfanout

import (
	"bufio"
	"context"
	"fmt"
	"github.com/google/uuid"
	"go-pipeline-stream/pkg/streams"
	"os"
	"time"
)

func Execute(size int) {
	generateSampleFile("sample", size)

	inputStream := inputStreamFromFile("sample")

	ctx := context.Background()

	m := MockedRepository{}

	initialTime := time.Now()
	lockedStream := streams.FanOutFanIn(ctx, 10, streams.CreateStreamRequest{
		Name:         "sample",
		Step:         "lock",
		Func:         m.LockPipeFunc,
		InputStream:  inputStream,
		ReceiveError: false,
	})

	getStream := streams.FanOutFanIn(ctx, 10, streams.CreateStreamRequest{
		Name:         "sample",
		Step:         "get",
		Func:         m.GetPipeFunc,
		InputStream:  lockedStream,
		ReceiveError: false,
	})

	setStream := streams.FanOutFanIn(ctx, 10, streams.CreateStreamRequest{
		Name:         "sample",
		Step:         "set",
		Func:         m.SetPipeFunc,
		InputStream:  getStream,
		ReceiveError: false,
	})

	unlockStream := streams.FanOutFanIn(ctx, 10, streams.CreateStreamRequest{
		Name:         "sample",
		Step:         "lock",
		Func:         m.UnlockPipeFunc,
		InputStream:  setStream,
		ReceiveError: false,
	})

	// reading the whole stream just to account the time that it ends
	for _ = range streams.OrDone(ctx, unlockStream) {

	}

	// printing duration
	fmt.Println(time.Since(initialTime).String())

}

func inputStreamFromFile(fileName string) streams.Stream {
	f, _ := os.Open(fileName)

	r := bufio.NewScanner(f)

	outputStream := make(chan streams.PipelineData)

	go func() {
		defer close(outputStream)
		for r.Scan() {
			outputStream <- streams.PipelineData{Value: r.Text()}
		}
	}()

	return outputStream
}

func generateSampleFile(fileName string, size int) {
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	for i := 0; i < size; i++ {
		_, err := w.WriteString(uuid.New().String())
		if err != nil {
			panic(err)
		}

		// fix to avoid writing a blank line at the end
		if i != size-1 {
			w.WriteString("\n")
		}
	}
}

func (m MockedRepository) LockPipeFunc(_ context.Context, input streams.PipelineData) (output streams.PipelineData, err error) {
	m.Lock()
	return input, nil
}

func (m MockedRepository) GetPipeFunc(_ context.Context, input streams.PipelineData) (output streams.PipelineData, err error) {
	m.Get()
	return input, nil
}

func (m MockedRepository) SetPipeFunc(_ context.Context, input streams.PipelineData) (output streams.PipelineData, err error) {
	m.Set()
	return input, nil
}

func (m MockedRepository) UnlockPipeFunc(_ context.Context, input streams.PipelineData) (output streams.PipelineData, err error) {
	m.Unlock()
	return input, nil
}

// sample implementation to emulate database times
type MockedRepository struct{}

func (m MockedRepository) Lock() {
	<-time.After(time.Millisecond * 20)
}

func (m MockedRepository) Get() {
	<-time.After(time.Millisecond * 40)
}

func (m MockedRepository) Set() {
	<-time.After(time.Millisecond * 40)
}

func (m MockedRepository) Unlock() {
	<-time.After(time.Millisecond * 20)
}
