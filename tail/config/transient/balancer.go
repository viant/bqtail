package transient

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
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

func (t Balancer) ProjectID(performance contract.ProjectPerformance) string {
	switch len(t.ProjectIDs) {
	case 0:
		return ""
	case 1:
		return t.ProjectIDs[0]
	}
	switch t.Strategy {
	case base.BalancerStrategyFallback:
		if len(performance) > 0 {
			for _, projectID := range t.ProjectIDs {
				perf, ok := performance[projectID]
				if !ok {
					return projectID
				}
				if perf.ActiveLoadCount() > t.MaxLoadJobs {
					return projectID
				}
			}
		}
	}
	index := int(uint(rand.NewSource(time.Now().UnixNano()).Int63()) % uint(len(t.ProjectIDs)))
	return t.ProjectIDs[index]
}
