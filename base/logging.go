package base

import (
	"encoding/json"
	"fmt"
	"github.com/viant/toolbox"
	"log"
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
		var aMap = map[string]interface{}{}
		if err := toolbox.DefaultConverter.AssignConverted(&aMap, message); err != nil {
			log.Println(err)
		}
		aMap = toolbox.DeleteEmptyKeys(aMap)
		JSON, err := json.Marshal(aMap)
		if err != nil {
			text = "." + fmt.Sprintf("%+v", message)
		} else {
			text = "." + string(JSON)
		}
	}

	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	fmt.Print(text)
}
