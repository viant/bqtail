package base

import (
	"fmt"
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

//backend errors states that retries may solve error but actually it never does.
const backendError = "backendError"
const internalError = "internal error"
const noFound = "Not found"
const accessDenied = "Error 403"
const tableFragment = "Table"

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
	return strings.Contains(message, fmt.Sprintf("%v", http.StatusServiceUnavailable))
}

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
	return strings.Contains(message, internalError)
}

//IsNotFoundError returns true if not found storage error
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	//exclude BigQuery table not found error
	if strings.Contains(message, tableFragment) {
		return false
	}
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
