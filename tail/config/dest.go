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
	Pattern          string `json:",omitempty"`
	compiled         *regexp.Regexp
	Schema           Schema            `json:",omitempty"`
	TransientDataset string            `json:",omitempty"`
	Transient        *Transient        `json:",omitempty"`
	UniqueColumns    []string          `json:",omitempty"`
	Transform        map[string]string `json:",omitempty"`
	SideInputs       []*SideInput      `json:",omitempty"`
	Override         *bool
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
	return nil
}

//Validate checks if destination is valid
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
	return nil
}

//ExpandTable returns expanded table
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
		fmt.Printf("compiled: %v\n", d.Pattern)
		if d.compiled == nil {
			d.compiled, err = regexp.Compile(d.Pattern)
			fmt.Printf("compiled: %v %+v\n", err, d.compiled)
			if err != nil {
				return "", err
			}
		}
		dest = expandWithPattern(d.compiled, source, dest)
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
func (d *Destination) TableReference(created time.Time, source string) (*bigquery.TableReference, error) {
	return d.CustomTableReference(d.Table, created, source)
}

//CustomTableReference returns custom table reference
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

//NewJobConfigurationLoad creates a new load request
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
