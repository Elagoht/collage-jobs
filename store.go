package jobs

import "context"

// store holds a queue's waiting items. The in-memory store is the only
// implementation: its items are lost when the process ends. It stays
// unexported until a persistent store exists, whose needs (a ctx on Push, an
// acknowledgement after handling) will reshape it.
type store[T any] interface {
	// Push adds item, or returns ErrQueueFull when the store is full. It
	// never blocks.
	Push(item T) error
	// Pop removes and returns the oldest item, blocking until there is one
	// or ctx ends, when it returns ctx's error. With ctx already done it takes
	// nothing, even when an item waits.
	Pop(ctx context.Context) (T, error)
	// Len returns how many items wait.
	Len() int
}

// memStore is a store on a buffered channel.
type memStore[T any] struct{ ch chan T }

var _ store[int] = (*memStore[int])(nil)

func newMemStore[T any](capacity int) *memStore[T] {
	return &memStore[T]{ch: make(chan T, capacity)}
}

func (s *memStore[T]) Push(item T) error {
	select {
	case s.ch <- item:
		return nil
	default:
		return ErrQueueFull
	}
}

func (s *memStore[T]) Pop(ctx context.Context) (T, error) {
	// Checked first: with an item waiting too, select would pick at random,
	// and a cancelled worker must take nothing more.
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	select {
	case item := <-s.ch:
		return item, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (s *memStore[T]) Len() int { return len(s.ch) }
