# ASG status tracking API schema

1. API will accept statuses updates through POST request with json file having 
following format:

```json
{
  "status": string,
  "biosample_id": list,
  "tol_id": string,
  "species_name": string
}
```
**status** field can have following values:
- sample_received
- sample_released_for_lab_processing
- sample_in_sequencing
- sample_in_assembly
- sample_recollection_required
2. Example
```json
{
  "status": "sample_received",
  "biosample_id": ["SAMEA13854414"],
  "tol_id": "xbThrPhas1",
  "species_name": "Thracia phaseolina"
}
```

3. Response

API will return JSON file in this format:
```json
{
  "status": string,
  "biosample_id": list,
  "tol_id": string,
  "species_name": string,
  "processing_status": string,
  "message": list
}
```

4. Example response
**processing_status** field can only have these values:
- success
- error
```json
{
  "status": "sample_received",
  "biosample_id": ["SAMEA13854414"],
  "tol_id": "xbThrPhas1",
  "species_name": "Thracia phaseolina",
  "processing_status": "success",
  "message": ["status was updated"]
}
```

5. Examples

Login request - please request credentials from
[dtol-dcc@ebi.ac.uk](mailto:dtol-dcc@ebi.ac.uk).
```http request
curl -X 'POST' \
  'https://portal.darwintreeoflife.org/statuses_update/login' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'username={user_name}&password={password}'
```
API returns results in the format:
```json
{"access_token":"access_token_string","refresh_token":"refresh_token_string"}
```

Update status request
```http request
curl -X 'POST' \
  'https://portal.darwintreeoflife.org/statuses_update/status_update/' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer {JSON access token from the step above}' \
  -H 'Content-Type: application/json' \
  -d '{
  "status": "string",
  "biosamlpe_id": [
    "string"
  ],
  "tol_id": "string",
  "species_name": "string",
  "processing_status": "string",
  "message": "string"
}'
```
