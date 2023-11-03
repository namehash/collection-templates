# Custom Collections

Custom collections should be stored in a JSONL file, where each separate line is a JSON object. The JSON object should follow a specific format, which is described below.

```json
{
  "commands": {
    "sort_names": "none"
  },
  "data": {
    "collection_id": "string",
    "collection_name": "string",
    "collection_description": "optional string",
    "collection_keywords": "list of strings, may be empty",
    "avatar_emoji": "optional string",
    "labels": [
      {
        "normalized_name": "string",
        "tokenized_name": "optional string"
      },
      ...
    ]
  }
}
```