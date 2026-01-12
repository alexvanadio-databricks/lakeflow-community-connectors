# Lakeflow Aha! Community Connector

This documentation provides setup instructions and reference information for the Aha! source connector.

## Prerequisites

- An Aha! account with API access enabled
- An API key (Bearer token) for authentication
- Your Aha! subdomain (e.g., `company` for `company.aha.io`)

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_key` | string | Yes | Aha! API key used for authentication (sent as Bearer token) | `abc123...` |
| `subdomain` | string | Yes | Your Aha! subdomain | `company` |
| `ideas_workflow_status` | string | No | Default workflow status filter for ideas | `under_consideration` |
| `max_ideas` | string | No | Cap on number of ideas fetched (useful for testing) | `100` |

### Table-Specific Options

The following options can be passed per-table via `table_configuration`. Include them in the `externalOptionsAllowList` connection parameter as a comma-separated string:

```
ideas_workflow_status,max_ideas,ideas_q,q
```

| Option | Description |
|--------|-------------|
| `ideas_workflow_status` | Filter ideas by workflow status (overrides connection default) |
| `max_ideas` | Limit number of ideas fetched (overrides connection default) |
| `ideas_q` / `q` | Search query to filter ideas by name |

### Obtaining Your API Key

1. Log in to your Aha! account at `https://<subdomain>.aha.io`
2. Click your profile picture in the top-right corner
3. Select **Settings** > **Personal** > **Developer** > **API keys**
4. Click **Generate new API key**
5. Copy the generated key and store it securely

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways:

**Via UI:**
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select the Aha! connector or create a new connection
3. Provide `api_key` and `subdomain`
4. Set `externalOptionsAllowList` to: `ideas_workflow_status,max_ideas,ideas_q,q`

**Via CLI:**
```bash
community-connector create_connection aha <CONNECTION_NAME> \
  -o '{"api_key":"<AHA_API_KEY>","subdomain":"<AHA_SUBDOMAIN>"}' \
  --spec <YOUR_REPO_URL>
```

## Supported Objects

The connector ingests three tables from Aha!:

| Table | Description | Primary Key | Cursor Field | Ingestion Type |
|-------|-------------|-------------|--------------|----------------|
| `ideas` | Product ideas submitted to Aha! | `id` | `updated_at` | CDC |
| `idea_proxy_votes` | Proxy votes (votes on behalf of customers/organizations) | `id` | `updated_at` | CDC |
| `idea_comments` | Comments on ideas | `id` | `updated_at` | CDC |

### Incremental Sync Behavior

The connector uses **CDC (Change Data Capture)** with the `updated_since` API parameter:

- **First run:** Fetches all ideas and their associated proxy votes and comments
- **Subsequent runs:** Only fetches ideas updated since the last sync

**Why child tables sync efficiently:** When a proxy vote or comment is added to an idea, Aha! updates the parent idea's `updated_at` timestamp. This means:
- We only track the ideas' `updated_at` values
- Updated ideas automatically include those with new votes or comments
- Child tables only need API calls for the filtered set of ideas

### Limitations

- **Deletes are not tracked:** If an idea is deleted in Aha!, it remains in downstream Databricks tables. The Aha! API does not expose deleted records.
- **First run is a full fetch:** There's no way to avoid fetching all data on the initial sync.

## Data Type Mapping

| Aha! Type | Spark Type |
|-----------|------------|
| String / Text | `StringType` |
| Integer / Count | `LongType` |
| Boolean | `BooleanType` |
| Nested Object | `StructType` |
| Array | `ArrayType` |
| Key-Value Pairs | `MapType(StringType, StringType)` |
| Timestamp (ISO 8601) | `StringType` (stored as string) |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the Aha! connector code.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` to include the tables you want to ingest:

```json
{
  "pipeline_spec": {
    "connection_name": "<CONNECTION_NAME>",
    "objects": [
      {
        "table": {
          "source_table": "ideas",
          "ideas_workflow_status": "under_consideration"
        }
      },
      {
        "table": {
          "source_table": "idea_proxy_votes"
        }
      },
      {
        "table": {
          "source_table": "idea_comments"
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

```bash
community-connector create_pipeline aha <PIPELINE_NAME> \
  -ps '{"connection_name":"<CONNECTION_NAME>","objects":[{"table":{"source_table":"ideas"}},{"table":{"source_table":"idea_proxy_votes"}},{"table":{"source_table":"idea_comments"}}]}' \
  --repo-url <YOUR_REPO_URL>

community-connector run_pipeline <PIPELINE_NAME>
```

### Best Practices

- **Start small:** Use `max_ideas` to limit initial syncs during testing
- **Filter by status:** Use `ideas_workflow_status` to sync only relevant ideas
- **Use incremental sync:** The CDC approach significantly reduces API calls after the first run
- **Monitor rate limits:** Aha! API has rate limits; the connector handles 429 responses with automatic retry

### Troubleshooting

**Authentication errors (401):**
- Verify your API key is valid and hasn't expired
- Ensure the API key has appropriate permissions

**Rate limiting (429):**
- The connector automatically retries with backoff
- If persistent, reduce sync frequency or use `max_ideas` to limit scope

**Missing data:**
- Check if ideas are filtered by `ideas_workflow_status`
- Verify the subdomain is correct

**Empty child tables:**
- Proxy votes require ideas to have endorsements enabled
- Comments only appear if users have commented on ideas

## References

- [Aha! API Documentation](https://www.aha.io/api)
- [Aha! API Authentication](https://www.aha.io/api#authentication)
- [Aha! Ideas API](https://www.aha.io/api/resources/ideas)

---

## Developer Notes

The sections below are for contributors developing or maintaining this connector.

### Files Required for Deployment

- `aha.py`: The connector implementation
- `_generated_aha_python_source.py`: Single-file deployable (Databricks SDP-friendly)
- `connector_spec.yaml`: Connection parameter and options allowlist spec

### Running Tests

```bash
pytest sources/aha/test/ -v --log-cli-level=INFO
```

- **Unit tests** (`test_aha_unit.py`): Use mocked fixtures, no network calls
- **Integration tests** (`test_aha_integration.py`): Hit live Aha! API, require `configs/dev_config.json`

### Regenerating the Deployable

Re-run whenever you change `aha.py`, `libs/utils.py`, or `pipeline/lakeflow_python_source.py`:

```bash
python3 tools/scripts/merge_python_source.py aha
```

### Updating connector_spec.yaml

Update when you add/remove/rename connector options in `aha.py`. Use the `/generate-connector-spec` skill or manually edit following the template.
