package tail

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/bigquery/v2"
	"testing"
)

func TestWrapRecoverJob(t *testing.T) {

		var useCases  = []struct {
			description string
			jobID string
			expect string
		}{
			{
				description:"regular job",
				jobID:"myjob",
				expect:"recover0001_myjob",
			},
			{
				description:"recver job",
				jobID:"recover0004_myjob",
				expect:"recover0005_myjob",
			},

		}


		for _, useCase := range useCases {
			actual := wrapRecoverJobID(useCase.jobID)
			assert.EqualValues(t, useCase.expect, actual, useCase.description)
		}

}


func Test_removeCorruptedURIs(t *testing.T) {

		var useCases  = []struct {
			description     string
			job             string
			expectCorrupted []string
			expectedValid []string
		}{
			{
				description:     "missing file in gs",
				expectCorrupted: []string{"gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro"},
				expectedValid:[]string{"gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log.2019-11-19_19-41.1.i-03d29a135680c7b13.gz-v0.avro"},
				job:`{
  "configuration": {
    "jobType": "LOAD",
    "load": {
      "createDisposition": "CREATE_IF_NEEDED",
      "destinationTable": {
        "datasetId": "temp",
        "projectId": "myproject",
        "tableId": "mytable"
      },
      "sourceFormat": "AVRO",
      "sourceUris": [
        "gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
        "gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log.2019-11-19_19-41.1.i-03d29a135680c7b13.gz-v0.avro"
      ],
      "useAvroLogicalTypes": true,
      "writeDisposition": "WRITE_TRUNCATE"
    }
  },
  "etag": "CPmxTyCVv2jOT55WwdVweg==",
  "id": "myproject:US.temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch",
  "jobReference": {
    "jobId": "temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch",
    "location": "US",
    "projectId": "myproject"
  },
  "kind": "bigquery#job",
  "selfLink": "https://www.googleapis.com/bigquery/v2/projects/myproject/jobs/temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch?location=US",
  "statistics": {
    "creationTime": "1574193994917",
    "endTime": "1574193995142",
    "startTime": "1574193995061"
  },
  "status": {
    "errorResult": {
      "message": "Not found: URI gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
      "reason": "notFound"
    },
    "errors": [
      {
        "message": "Not found: URI gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
        "reason": "notFound"
      }
    ],
    "state": "DONE"
  },
  "user_email": "myproject-cloud-function@myproject.iam.gserviceaccount.com"
}`,
			},


			{
				description:     "missing file in bigstore",
				expectCorrupted: []string{"gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro"},
				expectedValid:[]string{"gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log.2019-11-19_19-41.1.i-03d29a135680c7b13.gz-v0.avro"},
				job:`{
  "configuration": {
    "jobType": "LOAD",
    "load": {
      "createDisposition": "CREATE_IF_NEEDED",
      "destinationTable": {
        "datasetId": "temp",
        "projectId": "myproject",
        "tableId": "mytable"
      },
      "sourceFormat": "AVRO",
      "sourceUris": [
        "gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
        "gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log.2019-11-19_19-41.1.i-03d29a135680c7b13.gz-v0.avro"
      ],
      "useAvroLogicalTypes": true,
      "writeDisposition": "WRITE_TRUNCATE"
    }
  },
  "etag": "CPmxTyCVv2jOT55WwdVweg==",
  "id": "myproject:US.temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch",
  "jobReference": {
    "jobId": "temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch",
    "location": "US",
    "projectId": "myproject"
  },
  "kind": "bigquery#job",
  "selfLink": "https://www.googleapis.com/bigquery/v2/projects/myproject/jobs/temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch?location=US",
  "statistics": {
    "creationTime": "1574193994917",
    "endTime": "1574193995142",
    "startTime": "1574193995061"
  },
  "status": {
    "errorResult": {
      "message": "Not found: URI gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
      "reason": "notFound"
    },
    "errors": [
      {
        "message": "Not found: Files /bigstore/mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
        "reason": "notFound"
      }
    ],
    "state": "DONE"
  },
  "user_email": "myproject-cloud-function@myproject.iam.gserviceaccount.com"
}`,
			},


			{
				description:     "missing file in gs",
				expectCorrupted: []string{"gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro"},
				expectedValid:[]string{"gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log.2019-11-19_19-41.1.i-03d29a135680c7b13.gz-v0.avro"},
				job:`{
  "configuration": {
    "jobType": "LOAD",
    "load": {
      "createDisposition": "CREATE_IF_NEEDED",
      "destinationTable": {
        "datasetId": "temp",
        "projectId": "myproject",
        "tableId": "mytable"
      },
      "sourceFormat": "AVRO",
      "sourceUris": [
        "gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
        "gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log.2019-11-19_19-41.1.i-03d29a135680c7b13.gz-v0.avro"
      ],
      "useAvroLogicalTypes": true,
      "writeDisposition": "WRITE_TRUNCATE"
    }
  },
  "etag": "CPmxTyCVv2jOT55WwdVweg==",
  "id": "myproject:US.temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch",
  "jobReference": {
    "jobId": "temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch",
    "location": "US",
    "projectId": "myproject"
  },
  "kind": "bigquery#job",
  "selfLink": "https://www.googleapis.com/bigquery/v2/projects/myproject/jobs/temp--x_zzz_39_20191119_439770381788305--439770381788305--dispatch?location=US",
  "statistics": {
    "creationTime": "1574193994917",
    "endTime": "1574193995142",
    "startTime": "1574193995061"
  },
  "status": {
    "errorResult": {
      "message": "Not found: URI gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
      "reason": "notFound"
    },
    "errors": [
      {
        "message": "Not found: URI gs://mybucket/nobid/xlog.request/2019/11/19/19/xlog.request.log-3.2019-11-19_19-33.1.i-0c50bdd516f3eb445.gz-v0.avro",
        "reason": "notFound"
      }
    ],
    "state": "DONE"
  },
  "user_email": "myproject-cloud-function@myproject.iam.gserviceaccount.com"
}`,
			},



		}
		for _, useCase := range useCases {
			job := &bigquery.Job{}
			err := json.Unmarshal([]byte(useCase.job), &job)
			if ! assert.Nil(t, err, useCase.description) {
				continue
			}

			assert.Nil(t, err, useCase.description)
			corrupted, valid := removeCorruptedURIs(job)
			assert.EqualValues(t, useCase.expectCorrupted, corrupted, useCase.description)
			assert.EqualValues(t, useCase.expectedValid, valid, useCase.description)

		}

}
