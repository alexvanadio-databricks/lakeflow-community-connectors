# Aha! Lakeflow Community Connector

This folder contains the **Aha!** community connector implementation plus the **deployable artifacts** Databricks needs to run it.

## How Ingestion Works

The Aha! connector uses **CDC (Change Data Capture)** to efficiently sync data from Aha! to Databricks. Instead of fetching all data on every run, it tracks what has changed since the last sync.

### Tables

The connector ingests three tables:

| Table | Description |
|-------|-------------|
| `ideas` | Product ideas submitted to Aha! |
| `idea_proxy_votes` | Proxy votes (votes on behalf of customers/organizations) |
| `idea_comments` | Comments on ideas |

### Incremental Sync Behavior

**First run:** The connector fetches all ideas from Aha! and processes their associated proxy votes and comments. This initial sync may take longer depending on the volume of data.

**Subsequent runs:** The connector only fetches ideas that have been updated since the last sync. It uses the `updated_since` parameter on the Aha! API to filter results. This dramatically reduces the number of API calls and processing time.

### Why This Works for Child Tables

A key insight that enables efficient syncing: **when a proxy vote or comment is added to an idea, Aha! updates the parent idea's `updated_at` timestamp**. This means:

- We only need to track the ideas' `updated_at` values
- When we fetch ideas updated since the last sync, we automatically get ideas that have new votes or comments
- Child tables (proxy votes, comments) only need to be fetched for the filtered set of ideas

For example, if you have 10,000 ideas but only 50 were updated since the last sync, the connector will:
1. Fetch those 50 ideas (instead of 10,000)
2. Fetch proxy votes for only those 50 ideas (instead of 10,000 API calls)
3. Fetch comments for only those 50 ideas (instead of 10,000 API calls)

### Offset Handling

The connector is **stateless** - it does not persist or retrieve checkpoint data itself. The responsibility is split:

**What the connector does:**
- Receives `start_offset` as input (empty on first run)
- Uses it to filter Aha! API calls via `updated_since`
- Computes the maximum `updated_at` from returned records
- Returns the new offset in the response

**What Spark/Lakeflow does:**
- Persists the offset to checkpoint storage (cloud storage) after each successful batch
- Retrieves it and passes it back to the connector on the next run

This separation means the connector works correctly in any environment (local testing, Databricks, etc.) without depending on any specific storage mechanism.

### Limitations

- **Deletes are not tracked:** If an idea is deleted in Aha!, it will remain in the downstream Databricks tables. The Aha! API does not expose deleted records.
- **First run is a full fetch:** There's no way to avoid fetching all data on the initial sync.

---

## Deploying with the `community-connector` CLI (recommended)

### Prereqs
- Your fork/clone of this repo is **pushed** to a Git host Databricks can access.
- You have Databricks CLI/SDK auth configured locally (the `community-connector` tool uses the same auth).

### Specifying a Databricks workspace
If you have multiple Databricks CLI profiles configured, specify which workspace to use via the `DATABRICKS_CONFIG_PROFILE` environment variable:

```bash
DATABRICKS_CONFIG_PROFILE=my-profile community-connector create_connection ...
```

### Files that must be in your repo/branch
- `aha.py`: the connector implementation
- `_generated_aha_python_source.py`: **single-file deployable** (Databricks SDP-friendly)
- `connector_spec.yaml`: connection parameter + `externalOptionsAllowList` spec for UI/CLI validation

### Typical CLI flow
1. Install the CLI:

```bash
cd tools/community_connector
pip install -e .
```

2. Create a Unity Catalog connection (type: `GENERIC_LAKEFLOW_CONNECT`). The CLI validates options against `connector_spec.yaml` and auto-adds `externalOptionsAllowList`:

```bash
community-connector create_connection aha <CONNECTION_NAME> \
  -o '{"api_key":"<AHA_API_KEY>","subdomain":"<AHA_SUBDOMAIN>"}' \
  --spec <YOUR_REPO_URL>
```

3. Create and run the ingestion pipeline:

```bash
community-connector create_pipeline aha <PIPELINE_NAME> -n <CONNECTION_NAME> \
  --repo-url <YOUR_REPO_URL>

community-connector run_pipeline <PIPELINE_NAME>
```

## Running Tests

Run all tests (unit + integration) with logs visible:

```bash
pytest sources/aha/test/ -v --log-cli-level=INFO
```

- **Unit tests** (`test_aha_unit.py`): Use mocked fixtures, no network calls
- **Integration tests** (`test_aha_integration.py`): Hit live Aha! API, require `configs/dev_config.json` with valid credentials

Integration tests are bounded by `max_ideas=100` to keep runtime reasonable (~2-3 minutes).

## Regenerating the deployable `_generated_aha_python_source.py`

Databricks SDP does not reliably support Python module imports for sources, so this repo uses a merged, single-file artifact.

Re-run the merge script **any time you change**:
- `sources/aha/aha.py`
- `libs/utils.py`
- `pipeline/lakeflow_python_source.py`

Generate the file like this (from repo root):

```bash
python3 tools/scripts/merge_python_source.py aha
```

Then **commit the updated** `sources/aha/_generated_aha_python_source.py` so Databricks runs the same code you tested.

## Updating `connector_spec.yaml`

The `connector_spec.yaml` file defines:
1. **Connection parameters** - Options passed when creating the Unity Catalog connection (e.g., `api_key`, `subdomain`)
2. **External options allowlist** - Table-specific options that can be passed through at runtime (e.g., `ideas_workflow_status`, `max_ideas`)

This file is used by the `community-connector` CLI to validate connection options and auto-populate the `externalOptionsAllowList` parameter.

**When to update:** If you add/remove/rename connector options in `aha.py`'s `__init__` method or table-specific options accessed from `table_options`.

**How to update:**
- Use the `/generate-connector-spec` skill in Claude Code, OR
- Manually edit following the template at `prompts/generate_connector_spec_yaml.md`

**Note:** Schema changes (adding/removing table fields) do NOT require updating this file - it only defines connector-level and table-level *options*, not the data schema.
