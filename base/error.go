package base

import (
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

const backendError = "backendError"
const internalError = "internal error"

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
	return strings.Contains(message, backendError) || strings.Contains(message, internalError)
}

//IsRetryError returns true if backend error
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound {
			return true
		}
	}
	return false
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
	return false
}
