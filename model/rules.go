package model

import (
	"github.com/go-errors/errors"
	"net/url"
)

type Rules struct {
	MetaURL   string
	Items     []*Rule
	OnSuccess *Action
	OnFailure *Action
}

func (r *Rules) Init() {
	for _, rule := range r.Items {
		rule.Init()
		if r.OnFailure != nil && rule.OnFailure == nil {
			rule.OnFailure = r.OnFailure
		}
		if r.OnSuccess != nil && rule.OnSuccess == nil {
			rule.OnSuccess = r.OnSuccess
		}
	}
}

func (r *Rules) Validate() error {
	if len(r.Items) == 0 {
		return errors.New("rules.items were empty")
	}
	for _, rule := range r.Items {
		if err := rule.Validate(); err != nil {
			return err
		}
	}
	if r.MetaURL == "" && r.HasBatchMode() {
		return errors.New("rules.metaURL was empty")
	}
	return nil
}

func (r *Rules) HasBatchMode() bool {
	if len(r.Items) == 0 {
		return false
	}
	for _, rule := range r.Items {
		if rule.Sync.Mode == ModeBatch {
			return true
		}
	}
	return false
}

func (r *Rules) Match(URL string) (*Rule, error) {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}
	URLPath := parsedURL.Path
	for _, rule := range r.Items {
		if rule.Source.Matches(URLPath) {
			return rule, nil
		}
	}
	return nil, nil
}

func NewRules(metaURL string, rules ...*Rule) *Rules {
	return &Rules{
		Items: rules,
	}
}
