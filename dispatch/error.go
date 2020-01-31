package dispatch

import (
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

const notFoundFragment = "not found"

//IsContextError returns true if err is context releated
func IsContextError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "context")
}

//IsNotFound returns true if not found
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound {
			return true
		}
	}
	return strings.Contains(err.Error(), notFoundFragment)
}

//isProcessingError returns true if processing error
func isProcessingError(err error) bool {
	if err == nil {
		return false
	}
	return !(IsContextError(err) || IsContextError(err))
}
