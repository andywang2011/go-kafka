package kafka

import (
	"context"
	"io"
	"time"
)

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// sleepCtx waits for d or until ctx is done, whichever comes first.
// It returns ctx.Err() when interrupted by cancellation.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type buildResult[T any] struct {
	v   T
	err error
}

// buildWithCtx runs a blocking constructor (Sarama dials brokers without a
// context) and returns early with ctx.Err() when ctx is cancelled. A value that
// is produced after cancellation is closed so no connection leaks.
func buildWithCtx[T io.Closer](ctx context.Context, build func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	ch := make(chan buildResult[T], 1)
	go func() {
		v, err := build()
		ch <- buildResult[T]{v: v, err: err}
	}()
	select {
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.err == nil {
				_ = r.v.Close()
			}
		}()
		return zero, ctx.Err()
	}
}
