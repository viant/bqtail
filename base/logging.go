package base

import (
	"encoding/json"
	"fmt"
	"github.com/viant/toolbox"
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

//Log logs message
func Log(message interface{}) {

	text, ok := message.(string)
	if !ok {
		JSON, err := json.Marshal(message)
		if err == nil {
			text = "." + string(JSON)
		} else {
			aMap := map[string]interface{}{}
			_ = toolbox.DefaultConverter.AssignConverted(&aMap, message)
			JSON, err := json.Marshal(message)
			if err == nil {
				text = ". " + string(JSON)
			} else {
				text = fmt.Sprintf("%+v", message)
			}
		}
	}
	fmt.Println(text)
}
