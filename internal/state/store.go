package state

import (
	"maps"
	"sort"
	"sync"

	"github.com/sajad-soudani/twin-pipeline/pkg/models"
)

type Store struct {
	mu     sync.RWMutex
	assets map[string]*models.AssetState
}

func NewStore() *Store {
	return &Store{assets: make(map[string]*models.AssetState)}
}

func (s *Store) Update(data models.SensorData) models.AssetState {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, ok := s.assets[data.AssetID]
	if !ok {
		s.assets[data.AssetID] = &models.AssetState{
			AssetID: data.AssetID,
			Metrics: make(map[string]float64),
			Status:  "nominal",
		}
	}
	a.Metrics[data.Metric] = data.Value
	a.LastUpdate = data.Timestamp

	return clone(a) // nobody should be able to touch the system's state, only the system itself
}

func (s *Store) Get(assetID string) (models.AssetState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a, ok := s.assets[assetID]

	if !ok {
		return models.AssetState{}, false
	}

	return clone(a), true
}

func (s *Store) Snapshot() []models.AssetState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]models.AssetState, 0, len(s.assets))

	for _, v := range s.assets {
		res = append(res, clone(v))
	}

	sort.Slice(res, func(i, j int) bool { return res[i].AssetID < res[j].AssetID })

	return res

}

func clone(a *models.AssetState) models.AssetState {
	m := make(map[string]float64, len(a.Metrics))

	maps.Copy(m, a.Metrics)

	c := *a
	c.Metrics = m

	return c
}
