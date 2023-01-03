package status

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/schema"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"io/ioutil"
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
	data, err := fs.DownloadWithURL(ctx, f.Location)
	if err != nil {
		return err
	}
	if strings.HasSuffix(f.Location, ".gz") {
		gzReader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return err
		}
		data, err = ioutil.ReadAll(gzReader)
		gzReader.Close()
		if err != nil {
			return err
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	rowNo := 1
	fName := f.Name
	var schemaRecord map[string]interface{}
	var record = map[string]interface{}{}

	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)
	var line []byte
	for ; scanner.Scan(); rowNo++ {
		line = scanner.Bytes()
		if rowNo < f.Row {
			continue
		}

		done, err := f.addField(line, record, fName, rowNo)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	if len(f.Fields) == 0 && len(line) > 0 {
		_, err := f.addField(line, record, fName, rowNo)
		if err != nil {
			return err
		}
	}

	if len(f.Fields) == 0 {
		return errors.Errorf("failed to extract schema from field: %v at row: %v, %v ", f.Name, f.Row, schemaRecord)
	}
	return nil
}

func (f *Field) addField(line []byte, record map[string]interface{}, fName string, rowNo int) (bool, error) {
	if err := json.Unmarshal(line, &record); err != nil {
		return false, fmt.Errorf("failed to parse JSON: %v, %s", err, line)
	}
	fName, values := f.getFieldWithRecord(fName, record)
	value, ok := values[fName]
	if !ok || value == nil {
		return false, nil
	}
	var err error
	f.Fields, err = schema.New(record, "Added auto: from location:"+f.Location+fmt.Sprintf(", row: %v,", rowNo)+" at: %s")
	if err != nil {
		return false, err
	}
	isRepeated := false
	f.Type, isRepeated = schema.FieldType(value)
	if isRepeated {
		f.Mode = schema.ModeRepeated
	}
	return true, nil
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
