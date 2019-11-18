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

	//StatusDuplicate status duplicate
	StatusDuplicate = "duplicate"

	//StatusError response status
	StatusError = "error"

	//DispatchJob dispatch job name
	DispatchJob = "dispatch"
	//TailJob tail job name
	TailJob = "tail"
	//JobPrefix default job refix
	JobPrefix = "bq"
	//JobExt job extension
	JobExt = ".json"
	//ActionExt job extension
	ActionExt = ".run"
	//JobElement job path element
	JobElement = "-job"
	//PathElementSeparator path separator
	PathElementSeparator = "--"

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
	//JobIDExpr job id expression
	JobIDExpr = "$JobID"
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

	//DeferTaskURL deferred URL
	DeferTaskURL = "DeferTaskURL"

	//SourceKey source URI
	SourceKey = "Source"

	//SourceTableExpr source table epxression
	SourceTableExpr = "$SourceTable"
	//DestTableKey dest table key
	DestTableKey = "destTable"
	//DestTableExpr dest table expression
	DestTableExpr = "$DestTable"

	//ConfigEnvKey config env key
	ConfigEnvKey = "CONFIG"

	//MaxRetries
	MaxRetries = 3

	//RetrySleepInSec
	RetrySleepInSec = 3

	//DoneState done status
	DoneState = "DONE"

	//DoneState done status
	RunningState = "RUNNING"

	//DoneState done status
	PendigState = "PENDING"

	//DateLayout represents a date layout
	DateLayout = "2006-01-02_15"

	//actionPrefix
	actionPrefix = "/_dispatched_/"

	//taskPrefix
	TaskPrefix = "/_deferred_/"

	replayPrefix = "/_Replay/"
)
