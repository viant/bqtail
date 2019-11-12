package replay

import (
	"bqtail/base"
	"strings"
)

//JobID returns job ID for supplied URL
func JobID(baseURL string, URL string) string {
	encoded := strings.Trim(string(URL[len(baseURL):]), "/")
	encoded = string(encoded[:len(encoded)-5])
	jobID :=  base.EncodePathSeparator(encoded)
	return jobID
}
