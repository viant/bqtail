package base

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
)

//Process Extension
const (
	//JobExt job extension
	JobExt = ".json"
	//ActionExt job extension
	ActionExt = ".run"
	//WindowExt batch window extension file
	WindowExt = ".win"
	//LocationExt location extension
	LocationExt = ".loc"
)

//Process action
const (
	//ActionLoad load action
	ActionLoad = "load"
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
)

const (
	//URLsKey urls key
	URLsKey = "URLs"
	//ResponseKey response key
	ResponseKey = "Response"
	//RootKey
	RootKey = "Root"
	//JobIDKey job id key
	JobIDKey = "JobID"
	//JobSourceKey source table/sql
	JobSourceKey = "JobSource"
	//ErrorKey error key
	ErrorKey = "Error"
	//ConfigEnvKey config env key
	ConfigEnvKey = "CONFIG"
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
	//ErrorSuffix error suffix
	ErrorSuffix = "-error"
	//LoadPrefix - load job default prefix
	LoadPrefix = "/_load_/"
	//BqJobPrefix bg job default prefix
	BqJobPrefix = "/_bqjob_/"
	//BatchPrefix batch task default prefix
	BatchPrefix = "/_batch_/"
	//InvalidSchemaLocation invalid schema
	InvalidSchemaLocation = "invalid_schema"
	//DoneLoadSuffix load done suffix
	DoneLoadSuffix = "Done"
	//ActiveLoadSuffix active done suffix
	ActiveLoadSuffix = "Running"
)

//DateLayout represents a date layout
const DateLayout = "2006-01-02_15"

//Waits and retries
const (
	//MaxRetries defines max retries
	MaxRetries = 3
	//RetrySleepInSec sleep between retries
	RetrySleepInSec = 3
	//StorageListVisibilityDelay - list storage operation can be delay with actual put object state.
	StorageListVisibilityDelay = 4000
)
