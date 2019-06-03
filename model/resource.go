package model

import (
	"strings"
)

type Resource struct {
	Prefix string
	Ext    string
	//TODO regexpr matching
	Fragment []*Fragment
}

func (r *Resource) Matches(URLPath string) bool {
	if r.Prefix == "" {
		return strings.HasSuffix(URLPath, r.Ext)
	}
	return strings.HasPrefix(URLPath, r.Prefix) && strings.HasSuffix(URLPath, r.Ext)
}
