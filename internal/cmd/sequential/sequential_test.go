package sequential

import "testing"

func BenchmarkExecute(b *testing.B) {
	for n := 0; n < b.N; n++ {
		Execute(n)
	}
}

func TestExecute(t *testing.T) {
	Execute(1000)
}
