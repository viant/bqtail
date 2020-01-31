package config

import (
	"bqtail/dispatch/contract"
	"bqtail/tail/config/transient"
	"errors"
)

//Transient represents transient project, dataset settings
//When transient project is used (recommended, data is load to temp table and then copy to dest table)
type Transient struct {
	Dataset   string
	ProjectID string
	Alias     string
	Balancer  *transient.Balancer
}

func (t Transient) Validate() error {
	if t.Dataset == "" {
		return errors.New("Transient.Dataset was empty")
	}
	return nil
}

//ProjectID return job IDs
func (t Transient) JobProjectID(performance contract.ProjectPerformance) string {
	if t.Balancer == nil {
		return t.ProjectID
	}

	return t.Balancer.ProjectID(performance)
}
