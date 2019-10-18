package base

import (
	"os"
	"strings"
)

//LoggingEnvKey logging key
const LoggingEnvKey = "LOGGING"

//IsLoggingEnabled returns true if logging enabled
func IsLoggingEnabled() bool {
	return IsFnLoggingEnabled(LoggingEnvKey)
}

//IsFnLoggingEnabled returns true if logging is enabled
func IsFnLoggingEnabled(key string) bool {
	return strings.ToLower(os.Getenv(key)) == "true"
}
