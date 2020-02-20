package tail

import (
	"encoding/json"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/contract"
)

func (s *service) matchSourceWithRule(response *contract.Response, request *contract.Request) *config.Rule {
	response.RuleCount = len(s.config.Rules)
	var rule *config.Rule
	matched := s.config.Match(request.SourceURL)
	switch len(matched) {
	case 0:
	case 1:
		rule = matched[0]
	default:
		JSON, _ := json.Marshal(matched)
		response.Retriable = false
		response.Error = fmt.Sprintf("multi rule match currently not supported: %s", JSON)
		return nil
	}
	if rule == nil {
		response.Status = shared.StatusNoMatch
		return nil
	}
	response.Matched = true
	response.MatchedURL = request.SourceURL
	if rule.Disabled {
		response.Status = shared.StatusDisabled
		return nil
	}
	if base.IsLoggingEnabled() {
		fmt.Printf("rule: ")
		base.Log(rule)
	}
	return rule
}
