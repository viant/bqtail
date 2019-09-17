## Task service

Task service enables you to specify post job execution task for both success and failed job status.



The generic task format use the following structure

```json
{

    "OnSuccess": [
      {
        "Action": "ACTION_NAME_X1",
        "Request": {
            "Attr1": "Value1",
            "AttrN": "ValueM",
            "OnSuccess": [],
            "OnFailure": []
        }
      },
      {
        "Action": "ACTION_NAME_XN",
        "Request": {
          "Attr1": "Value1",
          "AttrN": "ValueM"
        }
     }
    ],
    "OnFailure": [
      {
        "Action": "ACTION_NAME_Y1",
        "Request": {
            "Attr1": "Value1",
            "AttrN": "ValueM"
        }
      },
      {
        "Action": "ACTION_NAME_YN",
        "Request": {
          "Attr1": "Value1",
          "AttrN": "ValueM",
          "OnSuccess": [],
          "OnFailure": []
        }
     }
    ]

}
```

The request can use the following expressions:

- $Error: error message
- $JobId: job ID
- $EvetID: originated cloud function event ID
- $SourceTable: Big Query job dataset.source table
- $DestTable: Big Query job dataset.dest table


The task services uses [Cloud Services](../service/README.md) with various actions. 

