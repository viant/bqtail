package batch

import (
	"bqtail/tail/config"
	"bqtail/tail/contract"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/file"
	"github.com/viant/afs/mem"
	"path"
	"strings"
	"testing"
	"time"
)

type testTransfer struct {
	URL     string
	eventID string
	modTime time.Time
}

type testTransfers []*testTransfer

func (t testTransfers) Resources(table string) []*asset.Resource {
	var result = make([]*asset.Resource, 0)
	for i, transfer := range t {
		resource := asset.NewFile(path.Join(table, transfer.eventID+transferableExtension), []byte(transfer.URL), file.DefaultFileOsMode)
		resource.ModTime = &t[i].modTime
		result = append(result, resource)
	}
	return result
}

type testWindows map[string]*Window

func (t testWindows) Resources(table string) []*asset.Resource {
	var result = make([]*asset.Resource, 0)
	for name, window := range t {
		data, _ := json.Marshal(window)
		resource := asset.NewFile(path.Join(table, name), data, file.DefaultFileOsMode)
		resource.ModTime = &window.End
		result = append(result, resource)
	}
	return result
}

func TestService_TryAcquireWindow(t *testing.T) {

	mgr := mem.Singleton()

	now := time.Now()

	var useCases = []struct {
		description  string
		stageURL     string
		request      *contract.Request
		route        *config.Rule
		transfers    testTransfers
		windows      testWindows
		expect       *Window
		expectWinURL string
		hasError     bool
	}{
		{
			description: "the first event instance - can acquire window",
			stageURL:    "mem://localhost/stage001",
			transfers: []*testTransfer{
				{
					eventID: "event1",
					URL:     "mem://localhost/data/file1.avro",
					modTime: now.Add(-10 * time.Second),
				},
				{
					eventID: "event2",
					URL:     "mem://localhost/data/file2.avro",
					modTime: now.Add(-9 * time.Second),
				},
				{
					eventID: "event3",
					URL:     "mem://localhost/data/file3.avro",
					modTime: now.Add(-8 * time.Second),
				},
			},
			request: contract.NewRequest("event1", "mem://localhost/data/file2.avro", now.Add(-10*time.Second)),
			route: &config.Rule{
				Batch: &config.Batch{
					Window: &config.Window{
						Duration: 30 * time.Second,
					},
				},
				Dest: &config.Destination{
					Table: "proj:dset:table1",
				},
			},
			expectWinURL: fmt.Sprintf("mem://localhost/stage001/proj:dset:table1/%v.win", now.Add(20*time.Second).UnixNano()),
			expect: &Window{
				Start:   now.Add(-10 * time.Second),
				End:     now.Add(20 * time.Second),
				EventID: "event1",
			},
		},

		{
			description: "the second event instance - can not acquire window",
			stageURL:    "mem://localhost/stage002",
			transfers: []*testTransfer{
				{
					eventID: "event1",
					URL:     "mem://localhost/data/file1.avro",
					modTime: now.Add(-10 * time.Second),
				},
				{
					eventID: "event2",
					URL:     "mem://localhost/data/file2.avro",
					modTime: now.Add(-9 * time.Second),
				},
				{
					eventID: "event3",
					URL:     "mem://localhost/data/file3.avro",
					modTime: now.Add(-8 * time.Second),
				},
			},
			request: contract.NewRequest("event2", "mem://localhost/data/file2.avro", now.Add(-9*time.Second)),
			route: &config.Rule{
				Batch: &config.Batch{
					Window: &config.Window{
						Duration: 30 * time.Second,
					},
				},
				Dest: &config.Destination{
					Table: "proj:dset:table1",
				},
			},
		},

		{
			description: "the first event instance in the  next window - can acquire window",
			stageURL:    "mem://localhost/stage003",
			transfers: []*testTransfer{
				{
					eventID: "event1",
					URL:     "mem://localhost/data/file1.avro",
					modTime: now.Add(-40 * time.Second),
				},
				{
					eventID: "event2",
					URL:     "mem://localhost/data/file2.avro",
					modTime: now.Add(-9 * time.Second),
				},
				{
					eventID: "event3",
					URL:     "mem://localhost/data/file3.avro",
					modTime: now.Add(-8 * time.Second),
				},
			},
			request: contract.NewRequest("event2", "mem://localhost/data/file2.avro", now.Add(-9*time.Second)),
			route: &config.Rule{
				Batch: &config.Batch{
					Window: &config.Window{
						Duration: 30 * time.Second,
					},
				},
				Dest: &config.Destination{
					Table: "proj:dset:table1",
				},
			},
			expectWinURL: fmt.Sprintf("mem://localhost/stage003/proj:dset:table1/%v.win", now.Add(21*time.Second).UnixNano()),
			expect: &Window{
				Start:   now.Add(-9 * time.Second),
				End:     now.Add(21 * time.Second),
				EventID: "event2",
			},
		},

		{
			description: "the first event instance in the  next window - can acquire window",
			stageURL:    "mem://localhost/stage004",
			transfers: []*testTransfer{
				{
					eventID: "event1",
					URL:     "mem://localhost/data/file1.avro",
					modTime: now.Add(-20 * time.Second),
				},

				{
					eventID: "event2",
					URL:     "mem://localhost/data/file2.avro",
					modTime: now.Add(-9 * time.Second),
				},
				{
					eventID: "event3",
					URL:     "mem://localhost/data/file3.avro",
					modTime: now.Add(-8 * time.Second),
				},
			},
			windows: testWindows{
				fmt.Sprintf("%v.win", now.Add(-15*time.Second).UnixNano()): &Window{
					EventID: "event0",
					Start:   now.Add(-45 * time.Second),
					End:     now.Add(-15 * time.Second),
				},
			},
			request: contract.NewRequest("event2", "mem://localhost/data/file2.avro", now.Add(-9*time.Second)),
			route: &config.Rule{
				Batch: &config.Batch{
					Window: &config.Window{
						Duration: 30 * time.Second,
					},
				},
				Dest: &config.Destination{
					Table: "proj:dset:table1",
				},
			},

			expectWinURL: fmt.Sprintf("mem://localhost/stage004/proj:dset:table1/%v.win", now.Add(21*time.Second).UnixNano()),
			expect: &Window{
				Start:   now.Add(-9 * time.Second),
				End:     now.Add(21 * time.Second),
				EventID: "event2",
			},
		},
		{

			description: "the second  event instance in the  next window - can not acquire window",
			stageURL:    "mem://localhost/stage005",
			transfers: []*testTransfer{
				{
					eventID: "event1",
					URL:     "mem://localhost/data/file1.avro",
					modTime: now.Add(-20 * time.Second),
				},

				{
					eventID: "event2",
					URL:     "mem://localhost/data/file2.avro",
					modTime: now.Add(-9 * time.Second),
				},
				{
					eventID: "event3",
					URL:     "mem://localhost/data/file3.avro",
					modTime: now.Add(-8 * time.Second),
				},
			},
			windows: testWindows{
				fmt.Sprintf("%v.win", now.Add(-15*time.Second).UnixNano()): &Window{
					EventID: "event0",
					Start:   now.Add(-45 * time.Second),
					End:     now.Add(-15 * time.Second),
				},
				fmt.Sprintf("%v.win", now.Add(16*time.Second).UnixNano()): &Window{
					EventID: "event00",
					Start:   now.Add(-14 * time.Second),
					End:     now.Add(16 * time.Second),
				},
			},
			request: contract.NewRequest("event2", "mem://localhost/data/file2.avro", now.Add(-9*time.Second)),
			route: &config.Rule{
				Batch: &config.Batch{
					Window: &config.Window{
						Duration: 30 * time.Second,
					},
				},
				Dest: &config.Destination{
					Table: "proj:dset:table1",
				},
			},
		},

		{
			description: "the first event instance - can acquire window",
			stageURL:    "mem://localhost/stage006",
			transfers: []*testTransfer{
				{
					eventID: "event1",
					URL:     "mem://localhost/data/file1.avro",
					modTime: now.Add(-10 * time.Second),
				},
				{
					eventID: "event2",
					URL:     "mem://localhost/data/file2.avro",
					modTime: now.Add(-10 * time.Second),
				},
			},
			request: contract.NewRequest("event1", "mem://localhost/data/file2.avro", now.Add(-10*time.Second)),
			route: &config.Rule{
				Batch: &config.Batch{
					Window: &config.Window{
						Duration: 30 * time.Second,
					},
				},
				Dest: &config.Destination{
					Table: "proj:dset:table1",
				},
			},
			expectWinURL: fmt.Sprintf("mem://localhost/stage006/proj:dset:table1/%v.win", now.Add(20*time.Second).UnixNano()),
			expect: &Window{
				Start:   now.Add(-10 * time.Second),
				End:     now.Add(20 * time.Second),
				EventID: "event1",
			},
		},
	}

	ctx := context.Background()
	for _, useCase := range useCases {
		storage := afs.New()

		setupResources := useCase.transfers.Resources(useCase.route.Dest.Table)
		if len(useCase.windows) > 0 {
			setupResources = append(setupResources, useCase.windows.Resources(useCase.route.Dest.Table)...)
		}
		err := asset.Create(mgr, useCase.stageURL, setupResources)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		for _, transfer := range useCase.transfers {
			_ = storage.Upload(ctx, transfer.URL, 0644, strings.NewReader("123"))
		}

		srv := New(useCase.stageURL, storage)
		var window *Window
		batchWindow, err := srv.TryAcquireWindow(ctx, useCase.request, useCase.route)
		if batchWindow != nil {
			window = batchWindow.Window
		}
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}


		if useCase.expect != nil {
			if !assert.NotNil(t, window, useCase.description) {
				continue
			}
			assert.EqualValues(t, useCase.expect.EventID, window.EventID, useCase.description)
			assert.EqualValues(t, useCase.expect.Start, window.Start, useCase.description)

		} else {
			assert.Nil(t, window, useCase.description)
		}

		if useCase.expectWinURL != "" {
			has, _ := storage.Exists(ctx, useCase.expectWinURL)
			assert.True(t, has, useCase.description+", window file no found: "+useCase.expectWinURL)
		}
	}

}
