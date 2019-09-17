package config

//Routes represents route slice
type Routes []*Route

//HasMatch returns the first match route
func (r Routes) Match(URL string) *Route {
	for i := range r {
		if r[i].HasMatch(URL) {
			return r[i]
		}
	}
	return nil
}

//Validate checks if routes are valid
func (r Routes) Validate() error {
	for i := range r {
		if err := r[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

//UsesBatch returns true if routes uses batch
func (r Routes) UsesBatch() bool {
	for i := range r {
		if r[i].Batch != nil {
			return true
		}
	}
	return false
}

//UsesAsync returns true if routes uses async mode
func (r Routes) UsesAsync() bool {
	for i := range r {
		if r[i].Async {
			return true
		}
	}
	return false
}
