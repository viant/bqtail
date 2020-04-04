package shared

import "time"

//Process status
const (
	//StatusOK response status
	StatusOK = "ok"
	//StatusNoMatch status no match
	StatusNoMatch = "noMatch"
	//StatusNotFound status not found
	StatusNotFound = "notFound"
	//StatusDisabled status rule disabled
	StatusDisabled = "disabled"
	//StatusError response status
	StatusError = "error"
	//StatusStalled status for unprocessed file
	StatusStalled = "stalled"

	//StatusPending pending status
	StatusPending = "pending"
)

//Process Extension
const (
	//JSONExt job extension
	JSONExt = ".json"
	//YAMLExt job extension
	YAMLExt = ".yaml"
	//ProcessExt job extension
	ProcessExt = ".run"
	//WindowExt batch window extension file
	WindowExt = ".win"
	//LocationExt location extension
	LocationExt = ".loc"
	//CounterExt counter file extension
	CounterExt = ".cnt"
)

//Process action
const (
	//ActionLoad load action
	ActionLoad = "load"
	//ActionReload re load action
	ActionReload = "reload"
	//ActionLoad action copy table
	ActionCopy = "copy"
	//ActionQuery query action
	ActionQuery = "query"
	//ActionExport action export
	ActionExport = "export"
	//ActionMove move storage file
	ActionMove = "move"
	//ActionDelete delete storage file
	ActionDelete = "delete"
	//ActionNotify slack notify
	ActionNotify = "notify"
	//ActionDrop drop table
	ActionDrop = "drop"
	//ActionCall http call action
	ActionCall = "call"
	//ActionPush action pubusb push
	ActionPush = "push"
)

//Actionable  action with action meta
var Actionable = map[string]bool{
	ActionLoad:   true,
	ActionReload: true,
	ActionCopy:   true,
	ActionQuery:  true,
	ActionExport: true,
	ActionDrop:   true,
	ActionCall:   true,
	ActionPush:   true,
}

const (
	//URLsKey urls key
	URLsKey = "URLs"

	//URLKey urls key
	URLKey = "URL"

	//LoadURIsVar load uris expression
	LoadURIsVar = "$LoadURIs"
	//LoadURIsKey load uris key
	LoadURIsKey = "LoadURIs"
	//SourceURLsKey SourceURLs key
	SourceURLsKey = "SourceURLs"

	//SourceURLKey source URL key
	SourceURLKey = "SourceURL"

	//ResponseKey response key
	ResponseKey = "Response"

	//JobIDKey job id key
	JobIDKey = "JobID"
	//JobSourceKey source table/sql
	JobSourceKey = "JobSource"
	//ErrorKey error key
	ErrorKey = "Error"
	//ConfigEnvKey config env key
	ConfigEnvKey = "CONFIG"

	//MaxRetriesEnvKey max reties env key
	MaxRetriesEnvKey = "MAX_RETRIES"
)

//BigQuery job status
const (
	//DoneState done status
	DoneState = "DONE"
	//RunningState done status
	RunningState = "RUNNING"
	//PendingState done status
	PendingState = "PENDING"
)

//Setting prefix/suffix
const (
	//ErrorExt error ext
	ErrorExt = ".err"
	//ResponseErrorExt
	ResponseErrorExt = ".rsp"

	//TempProjectPrefix represents temp project prefix
	TempProjectPrefix = "proj:"

	//LoadPrefix - load job default prefix
	LoadPrefix = "/_load_/"
	//PostJobPrefix bg job default prefix
	PostJobPrefix = "/_bqjob_/"

	//BatchPrefix batch task default prefix
	BatchPrefix = "/_batch_/"
	//InvalidSchemaLocation invalid schema
	InvalidSchemaLocation = "invalid_schema"
	//DoneLoadSuffix load done suffix
	DoneLoadSuffix = "Done"
	//ActiveLoadSuffix active done suffix
	ActiveLoadSuffix = "Running"

	//RetryCounterSubpath retry couter subpath
	RetryCounterSubpath = "retry/counter"

	//RetryDataSubpath retry data subpath
	RetryDataSubpath = "retry/data"
)

const (
	//BalancerStrategyRand randomly select project
	BalancerStrategyRand = "rand"
	//BalancerStrategyFallback select next project from the list if previous project hit limits
	BalancerStrategyFallback = "fallback"
)

//PerformanceFile defines job performance file
const PerformanceFile = "performance.json"

//DateLayout represents a date layout
const DateLayout = "2006-01-02_15"

const (
	//DefaultPrefix default prefix
	DefaultPrefix = "/_adhoc_/"
	//InMemoryStorageBaseURL in memory storage URL
	InMemoryStorageBaseURL = "mem://localhost/"

	//ServiceAccountCredentials
	ServiceAccountCredentials = "GOOGLE_APPLICATION_CREDENTIALS"

	//UserAgent bqtail user agent
	UserAgent = "Viant/BqTail"
)

//Waits and retries
const (
	//MaxRetries defines max retries
	MaxRetries = 4
	//StorageListVisibilityDelay - list storage operation can be delay with actual put object state.
	StorageListVisibilityDelay = 5000
)

//MaxReload default max load attempts (excluding corrupted files)
var MaxReload = 15

//StalledDuration default stalled duration
var StalledDuration = 90 * time.Minute

const (
	//PathElementSeparator path separator
	PathElementSeparator = "--"
	//StepModeDispach dispatch job name
	StepModeDispach = "dispatch"
	//StepModeTail tail job name
	StepModeTail = "tail"
	//StepModeNop - no post actions job
	StepModeNop = "nop"
)

const (
	//WriteDispositionTruncate remove then write all data
	WriteDispositionTruncate = "WRITE_TRUNCATE"
	//WriteDispositionAppend append data
	WriteDispositionAppend = "WRITE_APPEND"
)

//ClientSecretURL client secret
const ClientSecretURL = "mem://github.com/viant/bqtail/auth/key.json"
