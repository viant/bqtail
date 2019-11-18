package config

import (
	"bqtail/base"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
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
	Table string `json:"table,omitempty"`

	Partition string `json:"partition,omitempty"`
	//Pattern uses URI relative path (without leading backslash)
	bigquery.JobConfigurationLoad
	Pattern          string `json:",omitempty"`
	pattern          *regexp.Regexp
	Schema           Schema            `json:",omitempty"`
	TransientDataset string            `json:",omitempty"`
	UniqueColumns    []string          `json:",omitempty"`
	Transform        map[string]string `json:",omitempty"`
	Override         *bool
}

func (d Destination) HasSplit() bool {
	return d.Schema.Split != nil
}

func (d Destination) Clone() *Destination {
	return &Destination{
		Table:            d.Table,
		Pattern:          d.Pattern,
		pattern:          d.pattern,
		Schema:           d.Schema,
		TransientDataset: d.TransientDataset,
		UniqueColumns:    d.UniqueColumns,
	}
}

//Validate checks if destination is valid
func (d *Destination) Validate() error {
	if d.Table == "" {
		return fmt.Errorf("dest.Table was empty")
	}
	if d.Schema.Split != nil {
		if d.TransientDataset == "" {
			return fmt.Errorf("dest.Schema.Split requires dest.TransientDataset")
		}
		return d.Schema.Split.Validate()
	}
	return nil
}

//Expand returns sourced table
func (d *Destination) ExpandTable(table string, created time.Time, source string) (string, error) {
	return d.Expand(table, created, source)
}

//Expand returns sourced table
func (d *Destination) Expand(dest string, created time.Time, source string) (string, error) {
	var err error
	if count := strings.Count(dest, ModExpr); count > 0 {
		if dest, err = expandMod(dest, source, count); err != nil {
			return dest, err
		}
	}
	if count := strings.Count(dest, DateExpr); count > 0 {
		dest = expandDate(dest, created, count)
	}
	if d.Pattern != "" {
		if d.pattern == nil {
			d.pattern, err = regexp.Compile(d.Pattern)
			if err != nil {
				return "", err
			}
		}

		dest = expandWithPattern(d.pattern, source, dest)
	}
	return dest, err
}

//TableReference returns table reference, source table syntax: project:dataset:table
func (d *Destination) TableReference(created time.Time, source string) (*bigquery.TableReference, error) {
	return d.CustomTableReference(d.Table, created, source)
}

//TableReference returns table reference, source table syntax: project:dataset:table
func (d *Destination) CustomTableReference(table string, created time.Time, source string) (*bigquery.TableReference, error) {
	table, err := d.ExpandTable(table, created, source)
	if err != nil {
		return nil, err
	}
	reference, err := base.NewTableReference(table)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get table reference %v", table)
	}
	if d.Partition != "" {
		partition, err := d.Expand(d.Partition, created, source)
		if err != nil {
			return nil, err
		}
		if partition == "" {
			return nil, errors.Errorf("expanded partition was empty: %v", d.Partition)
		}
		reference.TableId = reference.TableId + "$" + partition
	}
	return reference, nil
}





func expandMod(table string, source string, count int) (string, error) {
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
		value := base.Hash(source) % int(modValue)
		table = strings.Replace(table, expression, fmt.Sprintf("%v", value), 1)
	}
	return table, nil
}

func expandDate(table string, created time.Time, count int) string {
	date := created.Format(dateLayout)
	return strings.Replace(table, DateExpr, date, count)
}

func expandWithPattern(expr *regexp.Regexp, sourceURL string, table string) string {
	_, URLPath := url.Base(sourceURL, file.Scheme)
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
	result := d.JobConfigurationLoad
	result.DestinationTable, err = d.TableReference(created, URIs[0])
	if err != nil {
		return nil, err
	}
	result.SourceUris = URIs
	return &result, nil
}
