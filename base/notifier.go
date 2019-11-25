package base

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"path"
	"time"
)

//Notify represent notify function
type Notify func(ctx context.Context, fs afs.Service, URL string)

//Notifier represents URL changes notifier
type Notifier struct {
	fs             afs.Service
	baseURL        string
	rules          *Resources
	checkFrequency time.Duration
	nextCheck      time.Time
	onChange       Notify
	onRemove       Notify
}

func (m *Notifier) isCheckDue(now time.Time) bool {
	if m.nextCheck.IsZero() || now.After(m.nextCheck) {
		m.nextCheck = now.Add(m.checkFrequency)
		return true
	}
	return false
}

func (m *Notifier) notify(ctx context.Context, currentSnapshot []storage.Object) (notified bool) {
	snapshot := indexRules(currentSnapshot)
	for URL, lastModified := range snapshot {
		modTime := m.rules.Get(URL)
		if modTime == nil {
			m.onChange(ctx, m.fs, URL)
			notified = true
			m.rules.Add(URL, lastModified)
			continue
		}
		if !modTime.Equal(lastModified) {
			notified = true
			m.onChange(ctx, m.fs, URL)
		}
	}
	removed := m.rules.GetMissing(snapshot)
	for _, URL := range removed {
		notified = true
		m.onRemove(ctx, m.fs, URL)
		m.rules.Remove(URL)
	}
	return notified
}

//Notify notifies any rule changes
func (m *Notifier) Notify(ctx context.Context, fs afs.Service) (bool, error) {
	if m.baseURL == "" {
		return false, nil
	}
	if !m.isCheckDue(time.Now()) {
		return false, nil
	}
	rules, err := fs.List(ctx, m.baseURL, option.NewRecursive(true))
	if err != nil {
		return false, err
	}
	return m.notify(ctx, rules), nil
}

func NewNotifier(baeURL string, checkFrequency time.Duration, fs afs.Service, onChanged, onRemoved Notify) *Notifier {
	if checkFrequency == 0 {
		checkFrequency = time.Minute
	}
	return &Notifier{
		fs:             fs,
		onChange:       onChanged,
		onRemove:       onRemoved,
		checkFrequency: checkFrequency,
		baseURL:        baeURL,
		rules:          NewResources(),
	}
}

func indexRules(rules []storage.Object) map[string]time.Time {
	var indexed = make(map[string]time.Time)
	for _, rule := range rules {
		if rule.IsDir() || path.Ext(rule.Name()) != ".json" {
			continue
		}
		indexed[rule.URL()] = rule.ModTime()
	}
	return indexed
}
