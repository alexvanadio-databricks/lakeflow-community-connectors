# Aha! Lakeflow Community Connector

This folder contains the **Aha!** community connector implementation plus the **deployable artifacts** Databricks needs to run it.

## Deploying with the `community-connector` CLI (recommended)

### Prereqs
- Your fork/clone of this repo is **pushed** to a Git host Databricks can access.
- You have Databricks CLI/SDK auth configured locally (the `community-connector` tool uses the same auth).

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
