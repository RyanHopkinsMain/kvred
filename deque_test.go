package db

import (
	"slices"
	"testing"
)

// TestDequeMatchesSlice drives the ring buffer and a plain slice with the same
// operations, across many wrap-arounds and growths, and checks they agree.
func TestDequeMatchesSlice(t *testing.T) {
	var d deque
	var model []string
	for step := 0; step < 5000; step++ {
		v := string(rune('a' + step%26))
		switch op := (step * 7919) % 5; {
		case op == 0 || op == 1:
			d.PushFront(v)
			model = append([]string{v}, model...)
		case op == 2:
			d.PushBack(v)
			model = append(model, v)
		case op == 3 && len(model) > 0:
			if got := d.PopFront(); got != model[0] {
				t.Fatalf("step %d: PopFront = %q, want %q", step, got, model[0])
			}
			model = model[1:]
		case op == 4 && len(model) > 0:
			if got := d.PopBack(); got != model[len(model)-1] {
				t.Fatalf("step %d: PopBack = %q, want %q", step, got, model[len(model)-1])
			}
			model = model[:len(model)-1]
		}
		if d.Len() != len(model) {
			t.Fatalf("step %d: Len = %d, want %d", step, d.Len(), len(model))
		}
	}
	if len(model) > 0 && !slices.Equal(d.Range(0, d.Len()-1), model) {
		t.Fatal("final contents differ")
	}
}
