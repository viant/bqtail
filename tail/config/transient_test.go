package config

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/tail/config/transient"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransient_JobProjectID(t *testing.T) {

	var useCases = []struct {
		description string
		transient   *Transient
		performance contract.ProjectPerformance
		expect      string
		expectRand  map[string]bool
		hasError    bool
	}{
		{
			description: "empty project",
			transient:   &Transient{},
		},
		{
			description: "single project",
			transient:   &Transient{ProjectID: "blah"},
			expect:      "blah",
		},

		{
			description: "random strategy project",
			transient: &Transient{ProjectID: "blah", Balancer: &transient.Balancer{
				Strategy:    base.BalancerStrategyRand,
				ProjectIDs:  []string{"p1", "p2"},
				MaxLoadJobs: 0,
			}},
			expectRand: map[string]bool{
				"p1": true,
				"p2": true,
			},
		},
		{
			description: "fallback strategy first project",
			transient: &Transient{ProjectID: "blah", Balancer: &transient.Balancer{
				Strategy:    base.BalancerStrategyFallback,
				ProjectIDs:  []string{"p1", "p2"},
				MaxLoadJobs: 0,
			}},
			expect: "p1",
		},
		{
			description: "fallback strategy project",
			performance: contract.ProjectPerformance{
				"p1": &contract.Performance{Running: &contract.Metrics{LoadJobs: 30}},
				"p2": &contract.Performance{Running: &contract.Metrics{LoadJobs: 10}},
			},
			transient: &Transient{ProjectID: "blah", Balancer: &transient.Balancer{

				Strategy:    base.BalancerStrategyFallback,
				ProjectIDs:  []string{"p1", "p2"},
				MaxLoadJobs: 20,
			}},
			expect: "p2",
		},
		{
			description: "multi fallback strategy project",
			performance: contract.ProjectPerformance{
				"p1": &contract.Performance{Running: &contract.Metrics{LoadJobs: 30}},
				"p2": &contract.Performance{Running: &contract.Metrics{LoadJobs: 10}},
				"p3": &contract.Performance{Running: &contract.Metrics{LoadJobs: 4}},
			},
			transient: &Transient{ProjectID: "blah", Balancer: &transient.Balancer{

				Strategy:    base.BalancerStrategyFallback,
				ProjectIDs:  []string{"p1", "p2", "p3"},
				MaxLoadJobs: 10,
			}},
			expect: "p3",
		},
	}

	for _, useCase := range useCases {

		if len(useCase.expectRand) > 0 {
			for i := 0; i < 10 && len(useCase.expectRand) > 0; i++ {
				actual := useCase.transient.JobProjectID(useCase.performance)
				delete(useCase.expectRand, actual)
			}
			assert.True(t, len(useCase.expectRand) == 0, useCase.description)
			continue
		}

		actual := useCase.transient.JobProjectID(useCase.performance)
		assert.Equal(t, useCase.expect, actual, useCase.description)
	}
}
