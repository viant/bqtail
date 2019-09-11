package config

import "bqtail/dispatch/contract"

//Route represents trigger routes
type Routes []*Route


//Match returns matched route or nil
func (r Routes) Match(job *contract.Job) *Route {
	for i := range r {
		if r[i].When.Match(job) {
			return r[i]
		}
	}
	return nil
}