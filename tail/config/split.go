package config

import (
	"github.com/pkg/errors"
)

//Split represents data split
type Split struct {
	TimeColumn     string
	ClusterColumns [] string
	Mapping        []*TableMapping
}

//Validate checks if split is valid
func (s *Split) Validate() error {
	if len(s.Mapping) == 0 {
		return errors.Errorf("mapping were empty")
	}
	for i := range s.Mapping {
		if err := s.Mapping[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}


//TableMapping represents table mapping
type TableMapping struct {
	When string
	Then string
}

//Validate checks if mapping is valid
func (m *TableMapping) Validate() error {
	if m.When == "" {
		return errors.Errorf("when was empty")
	}
	if m.Then == "" {
		return errors.Errorf("when was empty")
	}
	return nil
}

