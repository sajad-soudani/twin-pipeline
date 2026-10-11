package worker

import (
	"context"

	"github.com/sajad-soudani/twin-pipeline/internal"
	"github.com/sajad-soudani/twin-pipeline/internal/ingestion"
	"github.com/sajad-soudani/twin-pipeline/internal/state"
	"github.com/sajad-soudani/twin-pipeline/internal/topic"
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

			if topic.IsSubTopic(job.Topic, internal.TopicEngine) {
				ingestion.Engine(id, job.Payload, job.QoS, job.Retain, job.Properties, store)
			}

		}
	}
}
