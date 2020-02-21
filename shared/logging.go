package shared

import (
	"encoding/json"
	"fmt"
	"github.com/viant/toolbox"
	"log"
	"os"
	"strings"
)

//LoggingEnvKey logging key
const (
	LoggingEnvKey = "LOGGING"
	//LoggingLevelInfo info logging level
	LoggingLevelInfo = "info"
	//LoggingLevelDebug debug logging leve
	LoggingLevelDebug = "debug"
	//LoggingProgressChar progress char
	LoggingProgressChar = "."
	//LoggingProgressLineSize line size
	LoggingProgressLineSize = 40
)

var lastLogMessage string
var progressCharCount = uint32(0)

//IsDebugLoggingLevel returns true if logging enabled
func IsDebugLoggingLevel() bool {
	return isLoggingLevel(LoggingEnvKey, "true") || isLoggingLevel(LoggingEnvKey, LoggingLevelDebug)
}

//IsDebugLoggingLevel returns true if logging enabled
func IsInfoLoggingLevel() bool {
	return isLoggingLevel(LoggingEnvKey, LoggingLevelInfo)
}

//isLoggingLevel returns true if logging is enabled
func isLoggingLevel(key string, value string) bool {
	return strings.ToLower(os.Getenv(key)) == value
}

//LogProgress logs progress
func LogProgress() {
	sequence := ""
	if lastLogMessage != LoggingProgressChar || progressCharCount > LoggingProgressLineSize {
		progressCharCount = 0
		sequence += "\n"
	}
	progressCharCount++
	lastLogMessage = sequence + LoggingProgressChar
	fmt.Printf(LoggingProgressChar)
}

//LogF logs message template with parameters
func LogF(template string, params ...interface{}) {
	if lastLogMessage == LoggingProgressChar {
		fmt.Print("\n")
	}
	message := fmt.Sprintf(template, params...)
	lastLogMessage = message
	fmt.Print(message)
}

//LogLn logs message
func LogLn(message interface{}) {
	textMessage, ok := message.(string)
	if !ok {
		var aMap = map[string]interface{}{}
		if err := toolbox.DefaultConverter.AssignConverted(&aMap, message); err != nil {
			log.Println(err)
		}
		aMap = toolbox.DeleteEmptyKeys(aMap)
		JSON, err := json.Marshal(aMap)
		if err != nil {
			textMessage = fmt.Sprintf("%+v", message)
		} else {
			textMessage = "." + string(JSON)
		}
	}
	if !strings.HasSuffix(textMessage, "\n") {
		textMessage += "\n"
	}
	LogF(textMessage)
}
