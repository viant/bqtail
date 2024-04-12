package config

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/config/pattern"
	"github.com/viant/toolbox/data"
	"github.com/viant/toolbox/data/udf"
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
	HourExpr   = "$Hour"
	dateLayout = "20060102"
)

// Destination data ingestion destination
type Destination struct {
	Table     string `json:"table,omitempty"`
	Partition string `json:"partition,omitempty"`
	//Pattern uses URI relative path (without leading backslash)
	bigquery.JobConfigurationLoad
	Pattern            string           `json:",omitempty"`
	Parameters         []*pattern.Param `json:",omitempty"`
	compiled           *regexp.Regexp
	Schema             Schema            `json:",omitempty"`
	TransientDataset   string            `json:",omitempty"`
	Transient          *Transient        `json:",omitempty"`
	UniqueColumns      []string          `json:",omitempty"`
	Transform          map[string]string `json:",omitempty" description:"optional map of the source column to dest expression"`
	SideInputs         []*SideInput      `json:",omitempty"`
	Override           *bool
	AllowFieldAddition bool   `json:",omitempty"`
	Expiry             string `json:",omitempty"`
}

// HasTemplate
func (d *Destination) HasTemplate() bool {
	return d.Schema.Template != "" || (d.Transient != nil && d.Transient.Template != "")
}

// Params build pattern paramters
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
	udfs := data.NewMap()
	udf.Register(udfs)
	for _, param := range d.Parameters {
		paramValue := expandWithPattern(d.compiled, source, param.Expression)
		result[param.Name] = udfs.ExpandAsText(paramValue)
	}
	return result, nil
}

// HasSplit returns true if dest has split
func (d Destination) HasSplit() bool {
	return d.Schema.Split != nil
}

// Clone clones destination
func (d Destination) Clone() *Destination {
	cloned := &Destination{
		JobConfigurationLoad: d.JobConfigurationLoad,
		Table:                d.Table,
		Partition:            d.Partition,
		Pattern:              d.Pattern,
		Transform:            d.Transform,
		Parameters:           d.Parameters,
		compiled:             d.compiled,
		Schema:               d.Schema,
		TransientDataset:     d.TransientDataset,
		Transient:            d.Transient,
		UniqueColumns:        d.UniqueColumns,
		SideInputs:           d.SideInputs,
		Override:             d.Override,
		AllowFieldAddition:   d.AllowFieldAddition,
		Expiry:               d.Expiry,
	}

	if len(d.Transform) > 0 {
		cloned.Transform = make(map[string]string)
		for k, v := range d.Transform {
			cloned.Transform[k] = v
		}
	}

	return cloned
}

// Validate checks if destination is valid
func (d Destination) Validate() error {
	if d.Table == "" && ((d.Schema.Template == "" && !d.Schema.Autodetect) || d.Transient == nil) {
		return fmt.Errorf("dest.Table was empty")
	}
	if d.Schema.Split != nil {
		if d.Transient == nil || d.Transient.Dataset == "" {
			return fmt.Errorf("dest.Schema.Split requires dest.Transient.Dataset")
		}
		if err := d.Schema.Split.Validate(); err != nil {
			return err
		}
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
		if err := d.Transient.Validate(); err != nil {
			return err
		}
		if d.HasTransformation() && d.Schema.Autodetect {
			return errors.Errorf("autodetect schema is not supported with transformation options")
		}

		if d.Transient.CopyMethod != nil {
			switch *d.Transient.CopyMethod {
			case shared.CopyMethodCopy, shared.CopyMethodDML, shared.CopyMethodQuery:
			default:
				return errors.Errorf("invalid Transient.CopyMethod: %v, valid:[%v]", *d.Transient.CopyMethod, []string{
					shared.CopyMethodCopy, shared.CopyMethodDML, shared.CopyMethodQuery,
				})
			}
		}
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

// Init initialises destination
func (d *Destination) Init() error {
	if d == nil {
		return errors.Errorf("dest was nil")
	}
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
	if d.AllowFieldAddition && (d.SourceFormat == "AVRO" || d.SourceFormat == "PARQUET" || d.SourceFormat == "NEWLINE_DELIMITED_JSON") {
		if len(d.SchemaUpdateOptions) == 0 {
			d.SchemaUpdateOptions = []string{"ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"}
		}
		d.WriteDisposition = shared.WriteDispositionAppend
	}

	if d.HasTransformation() {
		if d.Transient.CopyMethod == nil || *d.Transient.CopyMethod == shared.CopyMethodCopy {
			d.Transient.CopyMethod = &shared.CopyMethodQuery
		}
	} else if d.Transient != nil && d.Transient.CopyMethod == nil {
		d.Transient.CopyMethod = &shared.CopyMethodCopy
	}
	return nil
}

// IsCopyMethodCopy returns true if copy method
func (d *Destination) IsCopyMethodCopy() bool {
	if d.Transient == nil || d.Transient.CopyMethod == nil {
		return true
	}
	return *d.Transient.CopyMethod == shared.CopyMethodCopy
}

// IsQueryCopyMethod returns true if query copy method
func (d *Destination) IsCopyMethodQuery() bool {
	if d.Transient == nil || d.Transient.CopyMethod == nil {
		return false
	}
	switch *d.Transient.CopyMethod {
	case shared.CopyMethodQuery, shared.CopyMethodDML:
		return true
	}
	return false
}

// HasTransformation returns true if dest requires transformation
func (d *Destination) HasTransformation() bool {
	return len(d.SideInputs) > 0 || len(d.Transform) > 0 || d.Schema.Split != nil || len(d.UniqueColumns) > 0
}

// ExpandTable returns expanded table
func (d *Destination) ExpandTable(table string, source *stage.Source) (string, error) {
	return d.Expand(table, source)
}

// Expand returns sourced table
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
	if count := strings.Count(dest, HourExpr); count > 0 {
		dest = expandHour(dest, source.Time, count)
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

// Match matched candidate table with the dest
func (d *Destination) Match(candidate string) bool {
	table := d.Table
	if table == "" {
		return candidate == ""
	}
	index := strings.Index(table, "$")
	if index != -1 {
		table = table[:index]
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

// TableReference returns table reference, source table syntax: project:dataset:table
func (d *Destination) TableReference(source *stage.Source) (*bigquery.TableReference, error) {
	return d.CustomTableReference(d.Table, source)
}

// CustomTableReference returns custom table reference
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
	date := created.UTC().Format(dateLayout)
	return strings.Replace(table, DateExpr, date, count)
}

func expandHour(table string, created time.Time, count int) string {
	hour := created.UTC().Format("03")
	return strings.Replace(table, HourExpr, hour, count)
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

// NewJobConfigurationLoad creates a new load request
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
