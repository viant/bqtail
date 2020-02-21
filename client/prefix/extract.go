package prefix

import (
	"github.com/viant/bqtail/tail/config"
)

//Extract extract data prefix
func Extract(rule *config.Rule) string {
	prefix := rule.When.Prefix
	if rule.When.Filter != "" {
		prefix = expandPattern(rule.When.Filter)
	}
	return prefix
}
