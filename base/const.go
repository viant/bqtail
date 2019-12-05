package base

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

	//JobExt job extension
	JobExt = ".json"
	//ActionExt job extension
	ActionExt = ".run"

	//WindowExt batch window extension file
	WindowExt = ".win"

	//LocationExt location extension
	LocationExt = ".loc"

	//SourceURLKey source key
	SourceURLKey = "SourceURL"

	//URLsKey urls key
	URLsKey = "URLs"

	//ResponseKey response key
	ResponseKey = "Response"

	//OnSuccessKey OnSuccess key
	OnSuccessKey = "OnSuccess"
	//OnFailureKey OnFailure key
	OnFailureKey = "OnFailure"

	//ErrorKey error key
	ErrorKey = "Error"
	//ErrorExpr error expression
	ErrorExpr = "$Error"
	//JobIDKey job id key
	JobIDKey = "JobID"

	//Info key
	InfoKey = "Info"
	//JobIDExpr job id expression
	JobIDExpr = "$GetJobID"
	//EventIDKey event key
	EventIDKey = "eventID"
	//EventIDExpr event id expression
	EventIDExpr = "$EventID"
	//SourceTableKey source table key
	SourceTableKey = "sourceTable"

	//UnclassifiedStatus
	UnclassifiedStatus = "unclassified"
	//StatusUnProcess status for unprocessed file
	StatusUnProcess = "unprocessed"
	//ErrorSuffix error suffix
	ErrorSuffix = "-error"

	//AsyncTaskURL deferred URL
	DeferTaskURL = "AsyncTaskURL"

	//SourceKey source URI
	SourceKey = "Source"

	//SourceTableExpr source table epxression
	SourceTableExpr = "$SourceTable"
	//DestTableKey dest table key
	DestTableKey = "jobDestTable"
	//DestTableExpr dest table expression
	DestTableExpr = "$DestTable"

	//ConfigEnvKey config env key
	ConfigEnvKey = "CONFIG"

	//MaxRetries
	MaxRetries = 3

	//RetrySleepInSec
	RetrySleepInSec = 3

	//StorageListVisibilityDelay
	StorageListVisibilityDelay = 4000

	//DoneState done status
	DoneState = "DONE"

	//DoneState done status
	RunningState = "RUNNING"

	//DoneState done status
	PendigState = "PENDING"

	//DateLayout represents a date layout
	DateLayout = "2006-01-02"

	//LoadPrefix
	LoadPrefix = "/_load_/"

	//BqJobPrefix
	BqJobPrefix = "/_bqjob_/"

	//BatchPrefix
	BatchPrefix = "/_batch_/"

	//InvalidSchemaLocation invalid schema
	InvalidSchemaLocation = "invalid_schema"

	//DoneLoadSuffix load done suffix
	DoneLoadSuffix = "Done"

	//ActiveLoadSuffix active done suffix
	ActiveLoadSuffix = "Running"
)
