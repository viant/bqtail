package base

import (
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

//backend errors states that retries may solve error but actually it never does.
const (
	backendError           = "backendError"
	internalError          = "internal error"
	internalServerResponse = "Internal Server Error"
	error500               = "Error 500"
	code500                = "code 500"
	noFound                = "Not found"
	accessDenied           = "Error 403"
	resetError             = "connection reset by peer"
	timeoutError           = "connection timed out"
	eofError               = "unexpected EOF"
	rateLimit              = "Exceeded rate limits"

	//TableFragment table fragment
	TableFragment = "Table"
)

//IsRetryError returns true if backend error
func IsRetryError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusServiceUnavailable || apiError.Code == http.StatusBadGateway {
			return true
		}
	}
	message := err.Error()
	return strings.Contains(message, fmt.Sprintf(" %v ", http.StatusServiceUnavailable)) ||
		strings.Contains(message, fmt.Sprintf(" %v ", http.StatusBadGateway)) ||
		strings.Contains(message, resetError) ||
		strings.Contains(message, eofError) ||
		strings.Contains(message, timeoutError) ||
		strings.Contains(message, rateLimit)
}

//IsBackendError returns true if backend errr
func IsBackendError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, backendError)
}

//IsInternalError returns true if internal error
func IsInternalError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusInternalServerError {
			return true
		}
	}
	message := err.Error()
	return strings.Contains(message, internalError) || strings.Contains(message, internalServerResponse) ||
		strings.Contains(message, error500) || strings.Contains(message, code500)
}

//IsNotFoundError returns true if not found storage error
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound {
			return true
		}
	}
	return strings.Contains(message, noFound)
}

//IsDuplicateJobError returns true if duplicate job error
func IsDuplicateJobError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusConflict {
			return true
		}
	}
	return false
}

//IsPermissionDenied returns true if permission job error
func IsPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusForbidden {
			return true
		}
	}
	message := err.Error()
	return strings.Contains(message, accessDenied)
}


//IsPreConditionError
func IsPreConditionError(err error) bool {
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