package status

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
)

//Field represents schema missing field
type Field struct {
	Name     string
	Location string
	Row      int
	Type     string
}

func (f *Field) AdjustType(ctx context.Context, fs afs.Service) error {
	reader, err := fs.DownloadWithURL(ctx, f.Location)
	if err != nil {
		return err
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	rowCount := 0
	f.Type = "STRING"
	for scanner.Scan() {
		rowCount++
		data := scanner.Bytes()
		if rowCount < f.Row {
			continue
		}
		var record = map[string]interface{}{}
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		value, ok := record[f.Name]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case float64:
			f.Type = "FLOAT64"
			if (v/10)*10 == v {
				f.Type = "INT64"
				return nil
			}
			return nil
		case int:
			f.Type = "INT64"
			return nil
		case bool:
			f.Type = "BOOLEAN"
			return nil
		case string:
			f.Type = "STRING"
			return nil
		default:
			//TODO add more data type support
			return errors.Errorf("unable to add filed, unsupported type: %T", value)
		}
	}
	return nil
}
