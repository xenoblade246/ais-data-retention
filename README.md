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