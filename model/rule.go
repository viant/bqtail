package model

import (
	"github.com/go-errors/errors"
	"github.com/viant/toolbox"
)

const MinuteUnit = "min"

var defaultFlushFrequency = &toolbox.Duration{
	Value: 90,
	Unit:  MinuteUnit,
}

type Rule struct {
	Source           Resource
	Dest             Table
	TransientDataset string
	Schema           *Schema
	Sync             Sync
	OnSuccess        *Action
	OnFailure        *Action
}

func (r *Rule) HasPostAction() bool {
	if r.OnSuccess == nil && r.OnFailure == nil {
		return false
	}
	if r.OnSuccess != nil && r.OnSuccess.Name == "" &&
		r.OnFailure != nil && r.OnFailure.Name == "" {
		return false
	}
	return true
}

func (r *Rule) Table(datafile *Datafile) *Table {
	//TODO dynamic expansion, etc..
	return &r.Dest
}

func (r *Rule) Init() {
	if r.Sync.Mode == "" {
		r.Sync.Mode = ModeIndividual
	} else if r.Sync.Mode == ModeBatch {
		if r.Sync.FlushFrequency.Unit == " " {
			r.Sync.FlushFrequency = defaultFlushFrequency
		}
	}
}

func (r *Rule) Validate() error {
	if r.Source.Ext == "" {
		return errors.New("source.ext was empty")
	}
	if r.Dest.TableID == "" {
		return errors.New("dest.tableID was empty")
	}
	if r.Dest.DatasetID == "" {
		return errors.New("dest.datasetID was empty")
	}
	return nil
}
