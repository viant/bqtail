package model

import "github.com/viant/toolbox"

type Mode string

//ModeIndividual individual sync mode
var ModeIndividual Mode = "individual"

//ModeBatch batch sync mode
var ModeBatch Mode = "batch"

type Sync struct {
	Mode           Mode
	FlushFrequency *toolbox.Duration
}
