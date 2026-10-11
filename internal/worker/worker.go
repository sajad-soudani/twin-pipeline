package worker

import (
	"context"

	"github.com/sajad-soudani/twin-pipeline/internal/state"
)

func StartWorkerPool(ctx context.Context, count int, jobs <-chan Job, store *state.Store) {
	for i := range count {
		go worker(ctx, i, jobs, store)
	}
}

func worker(ctx context.Context, id int, jobs <-chan Job, store *state.Store) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

		}
	}
}
