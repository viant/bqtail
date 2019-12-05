package info

type Destination struct {
	Table  string `json:",omitempty"`
	Shards int    `json:",omitempty"`
	Days   int    `json:",omitempty"`
}
