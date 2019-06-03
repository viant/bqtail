package bqtail

import (
	"bqtail/bq"
	"bqtail/gs"
	"bqtail/model"
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type Service interface {
	Tail(*Request) *Response
}

type service struct {
	Config    *Config
	bqService bq.Service
}

func (s *service) syncIndividual(rule *model.Rule, request *Request, response *Response) (err error) {
	var datafile *model.Datafile
	datafile, err = gs.Get(request.SourceURL)

	if err != nil {
		return err
	}
	datafile.OnFailure = rule.OnFailure
	datafile.OnSuccess = rule.OnSuccess
	defer func() {
		err = handlePostAction(datafile, err)
	}()
	dest := rule.Table(datafile)

	loadRequest := bq.NewLoadRequest(request.JobID(0), dest, true, true, request.SourceURL)
	s.updateTableSchema(rule, loadRequest)
	loadResponse := s.bqService.Load(loadRequest)
	if loadResponse.Job != nil {
		response.JobRefs = append(response.JobRefs, loadResponse.JobReference)
	}
	if loadResponse.Error != "" {
		err = fmt.Errorf("%v", loadResponse.Error)
		return err
	}
	if job := loadResponse.Job; job.Status != nil && job.Status.ErrorResult != nil {
		buf := new(bytes.Buffer)
		_ = json.NewEncoder(buf).Encode(job.Status.ErrorResult)
		err = fmt.Errorf("%v", buf.String())
	}
	return err
}

func (s *service) match(request *Request, response *Response) error {
	rule, err := s.Config.Rules.Match(request.SourceURL)
	if rule == nil {
		return err
	}
	response.Mode = string(rule.Sync.Mode)
	switch rule.Sync.Mode {
	case model.ModeIndividual:
		return s.syncIndividual(rule, request, response)
	case model.ModeBatch:
		return fmt.Errorf("sync mode %v not yet supported", rule.Sync.Mode)
	default:
		return fmt.Errorf("unknown sync '%v' mode, valid: %v, %v", rule.Sync.Mode, model.ModeIndividual, model.ModeBatch)
	}
}

func (s *service) Tail(request *Request) *Response {
	response := NewResponse()
	startTime := time.Now()
	defer response.SetTimeTaken(startTime)
	err := s.match(request, response)
	response.SetIfError(err)
	return response
}

func (s *service) updateTableSchema(rule *model.Rule, request *bq.LoadRequest) {
	schema := rule.Schema
	if schema == nil {
		return
	}
	if schema.Auto {
		request.Autodetect = true
	}
	//TODO handle transient, template, DDL etc ...
}

func New(config *Config) Service {
	return &service{
		Config:    config,
		bqService: bq.New(config.Credentials),
	}
}
