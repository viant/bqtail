package mon

import "strings"

//IsSchemaError returns true if schema error
func IsSchemaError(text string) bool {
	message := strings.ToLower(text)
	return strings.Contains(message, "field") || strings.Contains(message, "schema")
}

//IsCorruptedError returns true if corrupted error
func IsCorruptedError(text string) bool {
	message := strings.ToLower(text)
	return strings.Contains(message, "gs://") && strings.Contains(message, "location")
}
