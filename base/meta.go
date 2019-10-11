package base

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/storage"
	"sync"
	"time"
)

type Meta struct {
	baseURL        string
	routes         map[string]time.Time
	mutex          *sync.Mutex
	checkFrequency time.Duration
	nextCheck      time.Time
}

func (m *Meta) isCheckDue(now time.Time) bool {
	if m.nextCheck.IsZero() || now.After(m.nextCheck) {
		m.nextCheck = now.Add(m.checkFrequency)
		return true
	}
	return false
}

func (m *Meta) hasChanges(routes []storage.Object) bool {
	if len(routes) != len(m.routes) {
		return true
	}
	for _, route := range routes {
		modTime, ok := m.routes[route.URL()]
		if !ok {
			return true
		}
		if !modTime.Equal(route.ModTime()) {
			return true
		}
	}
	return false

}

//HasChanged returns true if resource under base URL have changed
func (m *Meta) HasChanged(ctx context.Context, fs afs.Service) (bool, error) {
	if m.baseURL == "" {
		return false, nil
	}
	if !m.isCheckDue(time.Now()) {
		return false, nil
	}

	basicMatcher, err := matcher.NewBasic("", ".json", "", nil)
	if err != nil {
		return false, err
	}
	routes, err := fs.List(ctx, m.baseURL, basicMatcher.Match)
	if err != nil {
		return false, err
	}
	if !m.hasChanges(routes) {
		return false, nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.routes = make(map[string]time.Time)
	for _, route := range routes {
		m.routes[route.URL()] = route.ModTime()
	}
	return true, nil
}

func NewMeta(baeURL string, checkFrequency time.Duration) *Meta {
	if checkFrequency == 0 {
		checkFrequency = time.Minute
	}
	return &Meta{
		checkFrequency: checkFrequency,
		mutex:          &sync.Mutex{},
		baseURL:        baeURL,
		routes:         make(map[string]time.Time),
	}
}
