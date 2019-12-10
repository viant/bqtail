package config

import (
	"github.com/pkg/errors"
	"strings"
)

//SideInput represents a side input
type SideInput struct {
	Table string
	From  string
	On    string
	Alias string
}

func (s SideInput) Validate() error {
	if s.Table == "" && s.From == "" {
		return errors.New("table was empty")
	}
	if s.Table != "" {
		if strings.Count(s.Table, ".") == 0 {
			return errors.Errorf("invalid table: %v, expected `projectId.datasetId.tableId`", s.Table)
		}
	}
	if s.On == "" {
		return errors.New("on (join criteria) was empty")
	}
	if s.Alias == "" {
		return errors.New("alias was empty")
	}
	if strings.Count(s.On, "t.") == 0 {
		return errors.Errorf("on criteria missing reference to main table with 't.' alias: %v", s.On)
	}
	if strings.Count(s.On, s.Alias+".") == 0 {
		return errors.Errorf("on criteria missing reference to side table with '%v.' alias: %v", s.Alias, s.On)
	}
	return nil
}
