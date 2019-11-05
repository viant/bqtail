package base

import "strings"

const backendError = "backendError"

//IsBackendError returns true if backend error
func IsBackendError(message string) bool {
	if message == "" {
		return false
	}
	return strings.Contains(message, backendError)
}
