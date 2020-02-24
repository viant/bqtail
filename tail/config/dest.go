package config

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/config/pattern"
	"github.com/viant/toolbox/data"
	"google.golang.org/api/bigquery/v2"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	//ModExpr expression computed from first table in batch or individual table
	ModExpr = "$Mod"
	//DateExpr data expression
	DateExpr   = "$Date"
	dateLayout = "20060102"
)

//Destination data ingestion destination
type Destination struct {
	Table     string `json:"table,omitempty"`
	Partition string `json:"partition,omitempty"`
	//Pattern uses URI relative path (without leading backslash)
	bigquery.JobConfigurationLoad
	Pattern          string           `json:",omitempty"`
	Parameters       []*pattern.Param `json:",omitempty"`
	compiled         *regexp.Regexp
	Schema           Schema            `json:",omitempty"`
	TransientDataset string            `json:",omitempty"`
	Transient        *Transient        `json:",omitempty"`
	UniqueColumns    []string          `json:",omitempty"`
	Transform        map[string]string `json:",omitempty"`
	SideInputs       []*SideInput      `json:",omitempty"`
	Override         *bool
}

//Params build pattern paramters
func (d Destination) Params(source string) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	if d.Pattern == "" || len(d.Parameters) == 0 {
		return result, nil
	}
	var err error
	if d.compiled == nil {
		d.compiled, err = regexp.Compile(d.Pattern)
		if err != nil {
			return nil, err
		}
	}
	for _, param := range d.Parameters {
		result[param.Name] = expandWithPattern(d.compiled, source, param.Expression)
	}
	return result, nil
}

//HasSplit returns true if dest has split
func (d Destination) HasSplit() bool {
	return d.Schema.Split != nil
}

//Clone clones destination
func (d Destination) Clone() *Destination {
	return &Destination{
		Table:            d.Table,
		Pattern:          d.Pattern,
		compiled:         d.compiled,
		Schema:           d.Schema,
		TransientDataset: d.TransientDataset,
		Transient:        d.Transient,
		UniqueColumns:    d.UniqueColumns,
		SideInputs:       d.SideInputs,
	}
}

//Validate checks if destination is valid
func (d Destination) Validate() error {
	if d.Table == "" {
		return fmt.Errorf("dest.Table was empty")
	}
	if d.Schema.Split != nil {
		if d.Transient == nil || d.Transient.Dataset == "" {
			return fmt.Errorf("dest.Schema.Split requires dest.Transient.Dataset")
		}
		return d.Schema.Split.Validate()
	}
	if len(d.SideInputs) > 0 {
		if d.Transient == nil || d.Transient.Dataset == "" {
			return errors.Errorf("sideInput %v requires transient.dataset", d.SideInputs[0].Table)
		}
		for _, sideInput := range d.SideInputs {
			if err := sideInput.Validate(d.Transient.Alias); err != nil {
				return err
			}
		}
	}
	if d.Transient != nil {
		return d.Transient.Validate()
	}

	if d.Table != "" {
		if _, err := base.NewTableReference(d.Table); err != nil {
			return errors.Wrapf(err, "invalid table: %v", d.Table)
		}
	}
	if d.Transient != nil && d.Transient.Template != "" {
		if _, err := base.NewTableReference(d.Transient.Template); err != nil {
			return errors.Wrapf(err, "invalid transient.template: %v", d.Transient.Template)
		}
	}
	if d.Schema.Template != "" {
		if _, err := base.NewTableReference(d.Schema.Template); err != nil {
			return errors.Wrapf(err, "invalid schema.template: %v", d.Schema.Template)
		}
	}
	return nil
}

//Init initialises destination
func (d *Destination) Init() error {
	if d.TransientDataset != "" {
		if d.Transient == nil {
			d.Transient = &Transient{Dataset: d.TransientDataset}
		}
		if d.Transient.Dataset == "" {
			d.Transient.Dataset = d.TransientDataset
		}
	}
	if d.Transient != nil && d.Transient.Alias == "" {
		d.Transient.Alias = "t"
	}
	if len(d.Transform) == 0 {
		d.Transform = make(map[string]string)
	}
	return nil
}

//ExpandTable returns expanded table
func (d *Destination) ExpandTable(table string, source *stage.Source) (string, error) {
	return d.Expand(table, source)
}

//Expand returns sourced table
func (d *Destination) Expand(dest string, source *stage.Source) (string, error) {
	var err error
	if count := strings.Count(dest, ModExpr); count > 0 {
		if dest, err = expandMod(dest, source.URL, count); err != nil {
			return dest, err
		}
	}
	if count := strings.Count(dest, DateExpr); count > 0 {
		dest = expandDate(dest, source.Time, count)
	}
	if d.Pattern != "" {
		if d.compiled == nil {
			d.compiled, err = regexp.Compile(d.Pattern)
			if err != nil {
				return "", err
			}
		}
		dest = expandWithPattern(d.compiled, source.URL, dest)
	}

	params, err := d.Params(source.URL)
	if err != nil {
		return "", err
	}
	if len(params) > 0 {
		paramsMap := data.Map(params)
		dest = paramsMap.ExpandAsText(dest)
	}
	return dest, err
}

func toComparableTable(table string) string {
	table = strings.Replace(table, ":", "_", len(table))
	table = strings.Replace(table, ".", "_", len(table))
	return table
}

//Match matched candidate table with the dest
func (d *Destination) Match(candidate string) bool {
	table := d.Table
	index := strings.Index(table, "$")
	if index != -1 {
		table = string(table[:index])
	}
	if strings.Contains(candidate, table) {
		return true
	}
	if ref, err := base.NewTableReference(table); err == nil {
		table = ref.DatasetId + "." + ref.TableId
	}
	if strings.Contains(candidate, table) {
		return true
	}
	table = toComparableTable(table)
	candidate = toComparableTable(candidate)
	return strings.Contains(candidate, table)
}

//TableReference returns table reference, source table syntax: project:dataset:table
func (d *Destination) TableReference(source *stage.Source) (*bigquery.TableReference, error) {
	return d.CustomTableReference(d.Table, source)
}

//CustomTableReference returns custom table reference
func (d *Destination) CustomTableReference(table string, source *stage.Source) (*bigquery.TableReference, error) {
	table, err := d.ExpandTable(table, source)
	if err != nil {
		return nil, err
	}
	reference, err := base.NewTableReference(table)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get table reference %v", table)
	}
	if d.Partition != "" {
		partition, err := d.Expand(d.Partition, source)
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

func expandWithPattern(expr *regexp.Regexp, sourceURL string, expression string) string {
	_, URLPath := url.Base(sourceURL, file.Scheme)
	matched := expr.FindStringSubmatch(URLPath)
	for i := 1; i < len(matched); i++ {
		key := fmt.Sprintf("$%v", i)
		count := strings.Count(expression, key)
		if count > 0 {
			expression = strings.Replace(expression, key, matched[i], count)
		}
	}
	return expression
}

//NewJobConfigurationLoad creates a new load request
func (d *Destination) NewJobConfigurationLoad(source *stage.Source, URIs ...string) (*bigquery.JobConfigurationLoad, error) {
	if len(URIs) == 0 {
		return nil, fmt.Errorf("URIs were empty")
	}
	var err error
	result := d.JobConfigurationLoad
	result.DestinationTable, err = d.TableReference(source)
	if err != nil {
		return nil, err
	}
	result.SourceUris = URIs
	return &result, nil
}
