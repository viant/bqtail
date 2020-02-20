package transient

import (
	"github.com/viant/bqtail/dispatch/contract"
	"github.com/viant/bqtail/shared"
	"math/rand"
	"time"
)

//Balancer represents projects balancer
type Balancer struct {
	//BalancingStrategy - rand - randomly selects project, fallback select next project in the list if previous project reached
	Strategy string `json:",omitempty"`

	//ProjectIDs
	ProjectIDs []string

	//MaxLoadJobs max load job for fallback strategy
	MaxLoadJobs int `json:",omitempty"`
}

//ProjectID returns project ID
func (t Balancer) ProjectID(performance contract.ProjectPerformance) string {
	switch len(t.ProjectIDs) {
	case 0:
		return ""
	case 1:
		return t.ProjectIDs[0]
	}
	switch t.Strategy {
	case shared.BalancerStrategyFallback:
		if projectID := t.selectPrioritizedProject(performance); projectID != "" {
			return projectID
		}
		return t.selectRandomProject()

	}

	projectID := t.selectRandomProject()
	if t.MaxLoadJobs == 0 || len(performance) == 0 {
		return projectID
	}
	perf, ok := performance[projectID]
	if !ok {
		return projectID
	}
	if perf.ActiveLoadCount() < t.MaxLoadJobs {
		return projectID
	}
	if underLoadProjectID := t.selectPrioritizedProject(performance); underLoadProjectID != "" {
		projectID = underLoadProjectID
	}
	return projectID
}

func (t Balancer) selectRandomProject() string {
	index := int(uint(rand.NewSource(time.Now().UnixNano()).Int63()) % uint(len(t.ProjectIDs)))
	projectID := t.ProjectIDs[index]
	return projectID
}

func (t Balancer) selectPrioritizedProject(performance contract.ProjectPerformance) string {
	if len(performance) == 0 {
		return t.ProjectIDs[0]
	}
	for _, projectID := range t.ProjectIDs {
		perf, ok := performance[projectID]
		if !ok {
			return projectID
		}
		if perf.ActiveLoadCount() < t.MaxLoadJobs {
			return projectID
		}
	}
	return ""
}
