package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/object"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"testing"
	"time"
)

var fs  = afs.New()

func TestSnapshot_GetWindowID(t *testing.T) {




		var useCases  = []struct {
			description string
			eventID string

			windowDuration time.Duration
			files []storage.Object

			expect string
		}{

			{
				description: "window in the past",
				eventID:"844224067128842",
				windowDuration:time.Second * 20,
				files:[]storage.Object{
					newWinowObject("1574102291100000000.win"),
					newWinowObject("1574102291114000000.win"),
					newObject("844224067128842.tnf", parseTime("2019-11-18T18:37:53Z",)),
				},
				expect:"1574102291100000000.win",
			},


			{
				description: "window ended with 1ms",
				eventID:"844224067128842",
				windowDuration:time.Second * 20,
				files:[]storage.Object{
					newWinowObject("1574281708000000000.win"),
					newObject("844224067128842.tnf", parseTime("2019-11-20T20:28:28.001Z",)),
				},
				expect:"",
			},
			{
				description: "window exactly ended",
				eventID:"844224067128842",
				windowDuration:time.Second * 20,
				files:[]storage.Object{
					newWinowObject("1574281708000000000.win"),
					newObject("844224067128842.tnf", parseTime("2019-11-20T20:28:28.009Z",)),
				},
				expect:"",
			},
		}

		for _, useCase := range useCases {
			snapshot := NewSnapshot(nil, useCase.eventID, fmt.Sprintf("%v.tnf", useCase.eventID), useCase.files, useCase.windowDuration)
			ownerID, err :=  snapshot.GetWindowID(context.Background() , useCase.windowDuration, fs)
			if ! assert.Nil(t, err, useCase.description) {
				continue
			}
			assert.Equal(t, useCase.expect, ownerID, useCase.description)
		}

}

func parseTime(literal string) time.Time {
	t, e := time.Parse(time.RFC3339Nano, literal)
	if e != nil {
		panic(e)
	}
	return t
}

func newWinowObject(name string) storage.Object {
	resource := asset.NewFile(name, []byte("1"), 0655)
	resource.ModTime, _ = windowNameToTime(name)
	URL := url.Join("mem://localhost/", name)

	window := &Window{
		EventID:name,
	}
	data, _ := json.Marshal(window)
	_ = fs.Upload(context.Background(), URL, 0655, bytes.NewReader(data))
	return object.New(URL, resource.Info(), resource)
}
