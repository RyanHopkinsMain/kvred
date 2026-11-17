package db

// deque is a growable ring buffer, so pushes and pops at either end are O(1).
// (The first version used a plain slice and prepended for LPUSH; redis-benchmark
// showed LPUSH at ~1.5k ops/sec as the list grew. This fixed it.)
type deque struct {
	buf  []string
	head int
	n    int
}

func (d *deque) Len() int { return d.n }

func (d *deque) grow() {
	size := max(8, 2*len(d.buf))
	buf := make([]string, size)
	for i := 0; i < d.n; i++ {
		buf[i] = d.At(i)
	}
	d.buf, d.head = buf, 0
}

func (d *deque) PushFront(v string) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = v
	d.n++
}

func (d *deque) PushBack(v string) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.buf[(d.head+d.n)%len(d.buf)] = v
	d.n++
}

func (d *deque) PopFront() string {
	v := d.buf[d.head]
	d.buf[d.head] = "" // let the GC have the string
	d.head = (d.head + 1) % len(d.buf)
	d.n--
	return v
}

func (d *deque) PopBack() string {
	i := (d.head + d.n - 1) % len(d.buf)
	v := d.buf[i]
	d.buf[i] = ""
	d.n--
	return v
}

// At returns the i-th element from the front (0 <= i < Len).
func (d *deque) At(i int) string { return d.buf[(d.head+i)%len(d.buf)] }

// Range copies elements [start, stop] inclusive.
func (d *deque) Range(start, stop int) []string {
	out := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, d.At(i))
	}
	return out
}
