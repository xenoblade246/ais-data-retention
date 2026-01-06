# AIS Web-App
## Contents:
- `aisUtil.py` -> Modified file from the GitLab repository; contains essential functions for the files below
- `gkg_uploader.py` -> Uploader of content from target GKG file into Elasticsearch
- `ilm_manager.py` -> Manages ILM permissions for Elasticsearch indices

## Errors:
- When trying to upload data from the GKG CSV file to Elasticsearch, after it finishes execution, the CLI simply hangs. The data is uploaded successfully to Elasticsearch though.
- When running the ILM manager file, it does manage to create policies. The newly created index, however, contains no data. May be an issue with age of data, since I only downloaded the GKG data today.

## Start-up
- Make sure Docker containers for Elastic-Kibana are running.
- Initialize a virtual env in the root directory, or install packages in the `requirements.txt` file.

Run the below block of code in Kibana dev tools before running the Python file:
```
PUT _index_template/gkg_data_template
{
  "index_patterns": ["gkg_data-*"],
  "template": {
    "settings": {
      "index.lifecycle.name": "ais_policy", 
      "index.lifecycle.rollover_alias": "gkg_data",
      "number_of_shards": 1,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "DATE": { "type": "date" },
        "PARSED_LOCATIONS": { "type": "nested" },
        "PARSED_COUNTS": { "type": "nested" },
        "PARSED_TONE": { "type": "object" }
        // ... (Include the rest of your properties from gkg_mapping.json here)
      }
    }
  },
  "priority": 200
}
```