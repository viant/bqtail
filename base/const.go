package base

const (
	StatusOK             = "ok"
	StatusError          = "error"
	DispatchJob          = "dispatch"
	TailJob              = "tail"
	JobPrefix            = "bq"
	JobExt               = ".json"
	JobElement           = "-job"
	PathElementSeparator = "--"
	SourceURLKey         = "SourceURL"
	SourceKey            = "Source"
	BodyKey              = "body"

	OnSuccessKey = "OnSuccess"
	OnFailureKey = "OnFailure"

	ErrorKey        = "error"
	ErrorExpr       = "$Error"
	JobIDKey        = "JobID"
	JobIDExpr       = "$JobID"
	EventIDKey      = "eventID"
	EventIDExpr     = "$EventID"
	SourceTableKey  = "sourceTable"
	SourceTableExpr = "$SourceTable"
	DestTableKey    = "destTable"
	DestTableExpr   = "$DestTable"
)
