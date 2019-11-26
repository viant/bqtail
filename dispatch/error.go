package dispatch

import "strings"

//IsContextError returns true if err is context releated
func IsContextError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "context")
}
