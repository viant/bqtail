package mon

import (
	"bqtail/base"
)

//RuleInfo represents workflow info with unprocessed files
type RuleInfo struct {
	base.Info
	ProcessedCount   int
	MaxProcessedSize int
	MinProcessedSize int
	UnprocessedCount int     `json:",omitempty"`
	DelaySec         int     `json:",omitempty"`
	Unprocessed      []*File `json:",omitempty"`
}

//NewRuleStatus create a workflow
func NewRuleStatus(info base.Info) *RuleInfo {
	return &RuleInfo{
		Info:        info,
		Unprocessed: make([]*File, 0),
	}
}
