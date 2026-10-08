package jobs

import "context"

// Store holds a queue's waiting items. The in-memory store is the only
// implementation: its items are lost when the process ends.
type Store[T any] interface {
	// Push adds item, or returns ErrQueueFull when the store is full. It
	// never blocks.
	Push(item T) error
	// Pop removes and returns the oldest item, blocking until there is one
	// or ctx ends, when it returns ctx's error.
	Pop(ctx context.Context) (T, error)
	// Len returns how many items wait.
	Len() int
}

// memStore is a Store on a buffered channel.
type memStore[T any] struct{ ch chan T }

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
	select {
	case item := <-s.ch:
		return item, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (s *memStore[T]) Len() int { return len(s.ch) }
