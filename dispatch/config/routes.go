package config

import (
	"bqtail/base"
)

//Route represents trigger routes
type Routes []*Route

//Match returns matched route or nil
func (r Routes) Match(job *base.Job) *Route {
	for i := range r {
		if r[i].When.Match(job) {
			return r[i]
		}
	}
	return nil
}
