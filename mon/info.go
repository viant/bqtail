package mon

import (
	"bqtail/mon/info"
	"bqtail/tail/config"
)

//Info represents ingestion info
type Info struct {
	Dest      *info.Destination `json:",omitempty"`
	Active    *info.Activity    `json:",omitempty"`
	Completed *info.Completion  `json:",omitempty"`
	Stalled   *info.Unprocessed `json:",omitempty"`
	Rule      *config.Rule      `json:",omitempty"`
}
