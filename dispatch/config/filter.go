package config

import (
	"bqtail/base"
	"regexp"
	"strings"
)

//Filter represents route filter
type Filter struct {
	Source string
	source *regexp.Regexp
	Dest   string
	dest   *regexp.Regexp
	Type   string
}

//Init initialises filter
func (f *Filter) Init() error {
	var err error
	if f.Source != "" {
		if f.source, err = regexp.Compile(f.Source); err != nil {
			return err
		}
	}
	if f.Dest != "" {
		if f.dest, err = regexp.Compile(f.Dest); err != nil {
			return err
		}
	}
	return nil
}

//Match return true if an event is matched
func (f *Filter) Match(event *base.Job) bool {
	matched := false
	if f.Type != "" {
		if strings.ToLower(f.Type) != strings.ToLower(event.Type()) {
			return false
		}
	}

	if f.source != nil {
		if !f.source.MatchString(event.Source()) {
			return false
		}
		matched = true
	}
	if f.dest != nil {
		if !f.dest.MatchString(event.Dest()) {
			return false
		}
		matched = true
	}
	return matched
}
