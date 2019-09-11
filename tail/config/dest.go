package config

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"bqtail/base"
	"google.golang.org/api/bigquery/v2"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	//Mod expression computed from first table in batch or individual table
	ModExpr    = "$Mod"
	DateExpr   = "$Date"
	dateLayout = "20060102"
)

type Destination struct {
	Table string
	//Pattern uses URI relative path (without leading backslash)

	Pattern string
	pattern *regexp.Regexp
	Schema  Schema

	TransientDataset string
	UniqueColumns    []string
}

func (d Destination) Clone() *Destination {
	return &Destination{
		Table:d.Table,
		Pattern:d.Pattern,
		pattern:d.pattern,
		Schema:d.Schema,
		TransientDataset:d.TransientDataset,
		UniqueColumns:d.UniqueColumns,
	}
}

//Validate checks if destination is valid
func (d *Destination) Validate() error {
	if d.Table == "" {
		return fmt.Errorf("dest.Table was empty")
	}
	return nil
}


//ExpandTable returns sourced table
func (d *Destination) ExpandTable(created time.Time, source string) (table string, err error) {
	table = d.Table
	if count := strings.Count(table, ModExpr); count > 0 {
		if table, err = expandMod(table, created, count); err != nil {
			return table, err
		}
	}
	if count := strings.Count(table, DateExpr); count > 0 {
		table = expandDate(table, created, count)
	}

	if d.Pattern != "" {
		if d.pattern == nil {
			d.pattern, err = regexp.Compile(d.Pattern)
			if err != nil {
				return "", err
			}
		}
		table = expandWithPattern(d.pattern, source, table)
	}

	return table, err
}

//TableReference returns table reference, source table syntax: project:dataset:table
func (d *Destination) TableReference(created time.Time, source string) (*bigquery.TableReference, error) {
	table, err := d.ExpandTable(created, source)
	if err != nil {
		return nil, err
	}
	return base.NewTableReference(table)
}

func expandMod(table string, created time.Time, count int) (string, error) {
	for i := 0; i < count; i++ {
		index := strings.Index(table, ModExpr)
		expression := string(table[index:])
		index = strings.Index(expression, ")")
		if index == -1 {
			return "", fmt.Errorf("invalid $Mod expression %v", table)
		}
		expression = string(expression[:index+1])
		mod := string(expression[len(ModExpr)+1 : len(expression)-1])
		modValue, err := strconv.ParseInt(mod, 10, 64)
		if err != nil {
			return table, errors.Wrapf(err, "invalid mod value: %v, in %v", modValue, expression)
		}
		value := created.Unix() % modValue
		table = strings.Replace(table, expression, fmt.Sprintf("%v", value), 1)
	}
	return table, nil
}

func expandDate(table string, created time.Time, count int) string {
	date := created.Format(dateLayout)
	return strings.Replace(table, DateExpr, date, count)
}

func expandWithPattern(expr *regexp.Regexp, sourceURL string, table string) (string) {
	_, URLPath := url.Base(sourceURL, file.Scheme)
	URLPath = strings.Trim(URLPath, "/")
	matched := expr.FindStringSubmatch(URLPath)
	for i := 1; i < len(matched); i++ {
		key := fmt.Sprintf("$%v", i)
		count := strings.Count(table, key)
		if count > 0 {
			table = strings.Replace(table, key, matched[i], count)
		}
	}
	return table
}

//NewLoadRequest creates a new load request
func (d *Destination) NewJobConfigurationLoad(created time.Time, URIs ...string) (*bigquery.JobConfigurationLoad, error) {
	if len(URIs) == 0 {
		return nil, fmt.Errorf("URIs were empty")
	}
	var err error
	result := &bigquery.JobConfigurationLoad{}
	result.DestinationTable, err = d.TableReference(created, URIs[0])
	if err != nil {
		return nil, err
	}
	result.SourceUris = URIs
	return result, nil
}
