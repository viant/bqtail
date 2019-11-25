package config

import (
	"bqtail/base"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

//Rule represents trigger routes
type Ruleset struct {
	RulesURL     string
	CheckInMs    int
	Rules        []*Rule
	notifier     *base.Notifier
	initialRules []*Rule
	inited       int32
}


//Match returns matched route or nil
func (r Ruleset) Match(job *base.Job) *Rule {
	if len(r.Rules) == 0 {
		return nil
	}

	for i := range r.Rules {
		if r.Rules[i].When.Match(job) {
			return r.Rules[i]
		}
	}
	return nil
}


//Validate checks if routes are valid
func (r *Ruleset) Validate() error {
	if len(r.Rules) == 0 {
		return nil
	}
	for _, rule := range r.Rules {
		if err := rule.Init(); err != nil {
			return err
		}
	}
	return nil
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

//Init initialises resources
func (r *Ruleset) Init(ctx context.Context, fs afs.Service, projectID string) error {
	if err := r.initRules(); err != nil {
		return err
	}
	checkFrequency := time.Duration(r.CheckInMs) * time.Millisecond
	r.notifier = base.NewNotifier(r.RulesURL, checkFrequency, fs, r.modify, r.remove)
	_, err := r.notifier.Notify(ctx, fs)
	return err
}



func (r *Ruleset) ReloadIfNeeded(ctx context.Context, fs afs.Service) (bool, error) {
	return r.notifier.Notify(ctx, fs)
}

func (c *Ruleset) loadRule(ctx context.Context, storage afs.Service, URL string) ([]*Rule, error) {
	reader, err := storage.DownloadWithURL(ctx, URL)
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
		if rules[i].Info.Workflow == "" {
			rules[i].Info.URL = URL
			rules[i].Info.Workflow = name
		}
	}
	return rules, nil
}

func (r *Ruleset) initRules() error {
	if atomic.CompareAndSwapInt32(&r.inited, 0, 1) {
		if len(r.Rules) > 0 {
			if err := r.Validate(); err != nil {
				return err
			}
			r.initialRules = r.Rules
		} else {
			r.initialRules = make([]*Rule, 0)
		}
	}
	return nil
}
