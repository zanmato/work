package work

import (
	"testing"
)

func TestMakeIdentifier(t *testing.T) {
	id := makeIdentifier()
	if len(id) < 10 {
		t.Errorf("expected a string of length 10 at least")
	}
}

func BenchmarkMakeIdentifier(b *testing.B) {
	for i := 0; i < b.N; i++ {
		makeIdentifier()
	}
}
