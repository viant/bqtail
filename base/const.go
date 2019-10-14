package base

const (
	//StatusOK response status
	StatusOK = "ok"
	//StatusNoMatch status no match
	StatusNoMatch = "noMatch"

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
	//JobElement job path element
	JobElement = "-job"
	//PathElementSeparator path separator
	PathElementSeparator = "--"
	//SourceURLKey source key
	SourceURLKey = "SourceURL"
	//SourceKey source key
	SourceKey = "Source"
	//BodyKey body key
	BodyKey = "body"

	//OnSuccessKey OnSuccess key
	OnSuccessKey = "OnSuccess"
	//OnFailureKey OnFailure key
	OnFailureKey = "OnFailure"

	//ErrorKey error key
	ErrorKey = "error"
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

	//DeferTaskURL deferred URL
	DeferTaskURL = "DeferTaskURL"

	//SourceTableExpr source table epxression
	SourceTableExpr = "$SourceTable"
	//DestTableKey dest table key
	DestTableKey = "destTable"
	//DestTableExpr dest table expression
	DestTableExpr = "$DestTable"
)
