package mon

import (
	"bqtail/base"
)

//RuleStatus represents workflow info with unprocessed files
type RuleStatus struct {
	base.Info
	ProcessedCount   int
	MaxProcessedSize int
	MinProcessedSize int
	UnprocessedCount int     `json:",omitempty"`
	DelaySec         int     `json:",omitempty"`
	Unprocessed      []*File `json:",omitempty"`
}

//NewRuleStatus create a workflow
func NewRuleStatus(info base.Info) *RuleStatus {
	return &RuleStatus{
		Info:        info,
		Unprocessed: make([]*File, 0),
	}
}
