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
	Inner bool
	Alias string
}

//Validate checks if side input is valid
func (s SideInput) Validate(alias string) error {
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
	if strings.Contains(s.On, "$") {
		return nil
	}
	if strings.Count(s.On, alias+".") == 0 {
		return errors.Errorf("on criteria missing reference to main table with '%v.' alias: %v", alias, s.On)
	}
	if strings.Count(s.On, s.Alias+".") == 0 {
		return errors.Errorf("on criteria missing reference to side table with '%v.' alias: %v", s.Alias, s.On)
	}
	return nil
}
