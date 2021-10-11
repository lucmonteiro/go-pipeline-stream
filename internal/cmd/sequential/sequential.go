package sequential

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
	generateSampleFile("sample_sequential", size)

	m := MockedRepository{}

	f, _ := os.Open("sample_sequential")
	initialTime := time.Now()

	r := bufio.NewScanner(f)
	for r.Scan() {
		m.Lock()
		m.Get()
		m.Set()
		m.Unlock()
	}

	// printing duration
	fmt.Println(time.Since(initialTime).String())
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
