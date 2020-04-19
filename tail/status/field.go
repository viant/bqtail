package status

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/schema"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Field represents schema missing field
type Field struct {
	Name     string
	Location string
	Row      int
	Type     string
	Mode     string
	Fields   []*bigquery.TableFieldSchema
}

func (f *Field) AdjustType(ctx context.Context, fs afs.Service) error {
	reader, err := fs.DownloadWithURL(ctx, f.Location)
	if err != nil {
		return err
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	rowNo := 1
	fName := f.Name
	var schemaRecord map[string]interface{}
	for ; scanner.Scan(); rowNo++ {
		data := scanner.Bytes()
		if rowNo < f.Row {
			continue
		}
		var record = map[string]interface{}{}
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		if err != nil {
			return errors.Wrapf(err, "unable to extract schema from %v", record)
		}
		if len(schemaRecord) == 0 {
			schemaRecord = record
		}
		fName, values := f.getFieldWithRecord(fName, record)
		value, ok := values[fName]
		if !ok || value == nil {
			continue
		}
		f.Fields, err = schema.New(record, "Added auto: from location:"+f.Location+fmt.Sprintf(", row: %v,", rowNo)+" at: %s")
		if err != nil {
			return err
		}
		isRepeated := false
		f.Type, isRepeated = schema.FieldType(value)
		if isRepeated {
			f.Mode = schema.ModeRepeated
		}
		break
	}
	if len(f.Fields) == 0{
		return errors.Errorf("failed to extract schema from field: %v at row: %v, %v ", f.Name, f.Row, schemaRecord)
	}
	return nil
}

func (f *Field) getFieldWithRecord(fName string, record map[string]interface{}) (string, map[string]interface{}) {

	for strings.Contains(fName, ".") {
		if index := strings.Index(fName, "."); index != -1 {
			parent := fName[:index]
			value, ok := record[parent]
			if ok {
				fName = fName[index+1:]
				if toolbox.IsSlice(value) {
					aSlice := toolbox.AsSlice(value)
					for _, val := range aSlice {
						if val == nil {
							continue
						}
						if toolbox.IsMap(val) {
							record = toolbox.AsMap(val)
							if v, ok := record[fName]; ok && v != nil {
								break
							}
						}
					}
				} else if toolbox.IsMap(value) {
					record = toolbox.AsMap(value)
				}
			}
		}
	}
	return fName, record
}
