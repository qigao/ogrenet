package utils

type RingBuffer[T any] struct {
	in  chan T
	out chan T
}

func NewRingBuffer[T any](inCh, outChn chan T) *RingBuffer[T] {
	return &RingBuffer[T]{
		in:  inCh,
		out: outChn,
	}
}

func (r *RingBuffer[T]) Run() {
	for v := range r.in {
		select {
		case r.out <- v:
		default:
			<-r.out // pop one item from outchan
			r.out <- v
		}
	}
	close(r.out)
}
