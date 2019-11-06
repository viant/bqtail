package base

import (
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

const backendError = "backendError"


//IsRetryError returns true if backend error
func IsRetryError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusServiceUnavailable {
			return true
		}
	}
	message := err.Error()
	return strings.Contains(message, backendError)
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