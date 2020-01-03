package batch

import (
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

func isPreConditionError(err error) bool {
	if err == nil {
		return false
	}
	origin := errors.Cause(err)
	if googleError, ok := origin.(*googleapi.Error); ok && googleError.Code == http.StatusPreconditionFailed {
		return true
	}
	message := err.Error()
	return strings.Contains(message, fmt.Sprintf(" %v", http.StatusPreconditionFailed))
}

func isRateError(err error) bool {
	if err == nil {
		return false
	}
	origin := errors.Cause(err)
	if googleError, ok := origin.(*googleapi.Error); ok && googleError.Code == http.StatusTooManyRequests {
		return true
	}
	message := err.Error()
	return strings.Contains(message, fmt.Sprintf(" %v", http.StatusTooManyRequests))
}
