## Task service

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
The [Cloud Services](../service/README.md) link details all currently supported actions.

