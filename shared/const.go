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
	//WindowExtScheduled batch window extension file for scheduled files
	WindowExtScheduled = ".wis"

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
	//ActionInsert action insert
	ActionInsert = "insert"

	//ActionTableExists action
	ActionTableExists = "tableExists"

	//ActionGroup grouping done action
	ActionGroup = "group"

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
	ActionInsert: true,
	ActionDrop:   true,
	ActionCall:   true,
	ActionPush:   true,
	ActionGroup:  true,
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

	//TriggerBucket represents trigger bucket
	TriggerBucket = "TriggerBucket"

	//SourceURLKey source URL key
	SourceURLKey = "SourceURL"

	//EventIDKey event ID key
	EventIDKey = "EventID"

	//DateKey date key
	DateKey = "Date"

	//HourKey hour key
	HourKey = "Hour"


	//GroupID group key
	GroupID = "GroupID"

	//DateSuffixLayout date layout suffix
	DateSuffixLayout = "20060102"

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

const (
	//PartitionIDExpr dynamically expand with partition_id value
	PartitionIDExpr = "PartitionID"
	//DollarSign represents dolar sign
	DollarSign = "DollarSign"
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

	//GroupExp group file extension
	GroupExp = ".grp"

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
	//StorageListVisibilityDelayMs - list storage operation can be delay with actual put object state.
	StorageListVisibilityDelayMs = 7000
	//MaxTriggerDelayMs	maximum trigger delay (2min)
	MaxTriggerDelayMs = 120000
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

//CopyMethodCopy represents a copy with Copy job
var CopyMethodCopy = "COPY"

//CopyMethodQuery represents a copy with Query job and dest table
var CopyMethodQuery = "QUERY"

//CopyMethodDML represents a copy with Query job and INSERT INTO AS SELECT DML
var CopyMethodDML = "DML"
