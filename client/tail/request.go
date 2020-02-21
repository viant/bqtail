package tail

import (
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/client/rule/build"
	"github.com/viant/bqtail/tail"
)

//Request represents tail request
type Request struct {
	Force     bool
	RuleURL   string
	Build     *build.Request
	Bucket    string
	SourceURL string
}

//Checks if request is valid
func (r Request) Validate() error {
	if r.SourceURL == "" {
		return errors.New("sourceURL was empty")
	}
	return nil
}

func (r *Request) Init(config *tail.Config) {
	if url.Scheme(r.SourceURL, file.Scheme) == gs.Scheme {
		if r.Build != nil {
			r.Bucket = r.Build.Bucket
		}
	}
	if r.Bucket == "" {
		r.Bucket = config.TriggerBucket
	}
}
