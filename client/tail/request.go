package tail

import (
	"github.com/pkg/errors"
	"github.com/viant/bqtail/client/option"
)

//Request represents tail request
type Request struct {
	*option.Options
}

//Checks if request is valid
func (r Request) Validate() error {
	if r.SourceURL == "" {
		return errors.New("sourceURL was empty")
	}
	if r.RuleURL == "" {
		return errors.New("ruleURL was empty")
	}
	return nil
}
