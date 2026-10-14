// Package state is the concurrent in-memory view of every asset.
package state

import (
	"cmp"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/sajad-soudani/twin-pipeline/internal/model"
)

// ErrTooManyAssets is returned when the asset cap is reached.
var ErrTooManyAssets = errors.New("state: asset limit reached")

type Store struct {
	mu        sync.RWMutex
	assets    map[string]*model.AssetState
	maxAssets int
}

// NewStore returns an empty store. maxAssets <= 0 means unlimited.
func NewStore(maxAssets int) *Store {
	return &Store{assets: make(map[string]*model.AssetState), maxAssets: maxAssets}
}

// Update records a reading. It reports applied=false (and no error) when the
// reading is OLDER than what we already hold for that metric.
//
// That check matters: a worker pool does not preserve message order, and a
// reconnecting session replays queued messages, so "last write wins" would
// let stale data overwrite fresh data.
func (s *Store) Update(assetID, metric string, r model.Reading) (applied bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, ok := s.assets[assetID]
	if !ok {
		if s.maxAssets > 0 && len(s.assets) >= s.maxAssets {
			return false, ErrTooManyAssets
		}
		a = &model.AssetState{
			AssetID: assetID,
			Metrics: make(map[string]model.Reading),
			Status:  model.StatusNominal,
		}
		s.assets[assetID] = a
	}

	if prev, ok := a.Metrics[metric]; ok && r.Timestamp.Before(prev.Timestamp) {
		return false, nil
	}
	a.Metrics[metric] = r
	if r.Timestamp.After(a.LastUpdate) {
		a.LastUpdate = r.Timestamp
	}
	return true, nil
}

// Get returns a copy; callers can't mutate the store through it.
func (s *Store) Get(assetID string) (model.AssetState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a, ok := s.assets[assetID]
	if !ok {
		return model.AssetState{}, false
	}
	return clone(a), true
}

// Snapshot returns copies of all assets sorted by ID.
func (s *Store) Snapshot() []model.AssetState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]model.AssetState, 0, len(s.assets))
	for _, a := range s.assets {
		out = append(out, clone(a))
	}
	slices.SortFunc(out, func(x, y model.AssetState) int { return cmp.Compare(x.AssetID, y.AssetID) })
	return out
}

func clone(a *model.AssetState) model.AssetState {
	c := *a
	c.Metrics = maps.Clone(a.Metrics) // Reading is a value type, so this is a deep copy
	return c
}
