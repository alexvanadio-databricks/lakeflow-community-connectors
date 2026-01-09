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


