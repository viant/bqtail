package info

//Destination represents bqtail data destination
type Destination struct {
	Table   string `json:",omitempty"`
	RuleURL string `json:",omitempty"`
}
