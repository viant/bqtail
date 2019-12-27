package config

import (
	"bqtail/base"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/url"
	"log"
	"strings"
	"time"
)

//Ruleset represents route slice
type Ruleset struct {
	RulesURL  string
	CheckInMs int
	Rules     []*Rule
	*base.Loader
}

func (r *Ruleset) modify(ctx context.Context, fs afs.Service, URL string) {
	loaded, err := r.loadRule(ctx, fs, URL)
	if err != nil {
		log.Printf("failed to load rule: %v: %v", URL, err)
	}
	var temp = make([]*Rule, 0)
	rules := r.Rules
	for i, rule := range rules {
		if rule.Info.URL == URL {
			continue
		}
		temp = append(temp, rules[i])
	}
	temp = append(temp, loaded...)
	r.Rules = temp
}

func (r *Ruleset) remove(ctx context.Context, fs afs.Service, URL string) {
	var temp = make([]*Rule, 0)
	rules := r.Rules
	for i, rule := range rules {
		if rule.Info.URL == URL {
			continue
		}
		temp = append(temp, rules[i])
	}
	r.Rules = temp
}

//Get returns  a rule for URL
func (r *Ruleset) Get(ctx context.Context, URL string, filter *matcher.Basic) *Rule {
	rules := r.Rules
	for i, rule := range rules {
		if rule.Info.URL == URL && rule.When.Prefix == filter.Prefix &&
			rule.When.Suffix == filter.Suffix &&
			rule.When.Filter == filter.Filter {
			return rules[i]
		}
	}
	return nil
}

//Match returns the match rules
func (r Ruleset) Match(URL string) []*Rule {
	if len(r.Rules) == 0 {
		return nil
	}
	var matched = make([]*Rule, 0)
	for i := range r.Rules {
		if r.Rules[i].HasMatch(URL) {
			matched = append(matched, r.Rules[i])
		}
	}
	return matched
}

//MatchByTable returns the first match route
func (r Ruleset) MatchByTable(table string) *Rule {
	if len(r.Rules) == 0 {
		return nil
	}

	for i := range r.Rules {
		if r.Rules[i].Dest.Match(table) {
			return r.Rules[i]
		}
	}
	return nil
}

//Validate checks if routes are valid
func (r Ruleset) Validate() error {
	if len(r.Rules) == 0 {
		return nil
	}
	for i := range r.Rules {
		if err := r.Rules[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

//UsesBatchInSyncMode returns true if routes uses batch
func (r Ruleset) UsesBatchInSyncMode() bool {
	if len(r.Rules) == 0 {
		return false
	}
	for i := range r.Rules {
		if r.Rules[i].Batch != nil && r.Rules[i].IsSyncMode() {
			return true
		}
	}
	return false
}

//UsesAsync returns true if routes uses async mode
func (r Ruleset) UsesAsync() bool {
	if len(r.Rules) == 0 {
		return false
	}
	for i := range r.Rules {
		if r.Rules[i].Async {
			return true
		}
	}
	return false
}

//Init initialises resources
func (r *Ruleset) Init(ctx context.Context, fs afs.Service, projectID string) error {
	if err := r.initRules(); err != nil {
		return err
	}
	checkFrequency := time.Duration(r.CheckInMs) * time.Millisecond
	r.Loader = base.NewLoader(r.RulesURL, checkFrequency, fs, r.modify, r.remove)
	_, err := r.Loader.Notify(ctx, fs)
	return err
}

//ReloadIfNeeded reloads rule if there is a change
func (r *Ruleset) ReloadIfNeeded(ctx context.Context, fs afs.Service) (bool, error) {
	return r.Loader.Notify(ctx, fs)
}

func (r *Ruleset) loadRule(ctx context.Context, fs afs.Service, URL string) ([]*Rule, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load resource: %v", URL)
	}
	defer func() {
		_ = reader.Close()
	}()
	rules := make([]*Rule, 0)

	err = json.NewDecoder(reader).Decode(&rules)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode: %v", URL)
	}
	transientRoutes := Ruleset{Rules: rules}
	if err := transientRoutes.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid rule: %v", URL)
	}
	_, name := url.Split(URL, "")
	if strings.HasSuffix(name, ".json") {
		name = string(name[:len(name)-5])
	}

	for i := range rules {
		rules[i].Info.URL = URL
		if rules[i].Info.Workflow == "" {
			rules[i].Info.Workflow = name
		}
		if err := rules[i].Dest.Init(); err != nil {
			return nil, err
		}
		if err := rules[i].Actions.Init(ctx, fs); err != nil {
			return nil, errors.Wrap(err, "failed to initialises pose action")
		}

	}
	return rules, nil
}

func (r *Ruleset) initRules() error {
	if len(r.Rules) > 0 {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	return nil
}
