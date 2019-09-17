package slack

import (
	"bqtail/service/secret"
	"bqtail/task"
	"github.com/viant/afs"
)

//Service represents storage service
type Service interface {
	task.Service
}

type service struct {
	projectID string
	Region    string
	Secret    secret.Service
	Storage   afs.Service
}

//New creates slack service
func New(region, projectID string, storageService afs.Service, secretService secret.Service) Service {
	return &service{
		Region:    region,
		projectID: projectID,
		Secret:    secretService,
		Storage:   storageService,
	}
}
