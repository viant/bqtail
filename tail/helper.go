package tail

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

const recoverJobPrefix = "recover"

//wrapRecoverJobID wrap recover with recover prefix and attempts
func wrapRecoverJobID(jobID string) string {
	attempt := 1
	if strings.HasPrefix(jobID, recoverJobPrefix) {
		offset := len(recoverJobPrefix)
		if offset + 4 < len(jobID) {
			attemptCounter := string(jobID[offset : offset +4])
			attempt = toolbox.AsInt(attemptCounter) +1
		}
		jobID = string(jobID[offset +5:])
	}
	return fmt.Sprintf(recoverJobPrefix + "%04d_%v", attempt, jobID)
}
