# Plan: Convert Aha! Connector from Snapshot to CDC

## Summary

Convert the Aha! connector from snapshot-based ingestion to CDC (Change Data Capture) mode. All three tables will use the `updated_since` API parameter to fetch only ideas (and their children) that have changed since the last sync.

## Confirmed Behavior

**Empirically tested:** Adding a proxy vote to an idea DOES update the parent idea's `updated_at` timestamp. This means we can trust `updated_since` filtering to capture:
- New/edited ideas
- Ideas with new/edited comments
- Ideas with new/edited proxy votes

## Current State

- **Ingestion type**: `snapshot` for all 3 tables
- **Offset handling**: Returns empty `{}`, ignores `start_offset`
- **cursor_field**: `None` for all tables
- **Behavior**: Every run fetches ALL ideas, then iterates through each for comments/votes

## Target State

- **Ingestion type**: `cdc` for all 3 tables
- **Offset handling**: Persists `{"updated_since": "<ISO8601_timestamp>"}`
- **cursor_field**: `updated_at` for all tables
- **Behavior**:
  - First run: Fetch all ideas (no filter), process all children
  - Subsequent runs: Fetch only ideas with `updated_at > last_checkpoint`, process only those children

## Efficiency Gain

| Scenario | Snapshot | CDC |
|----------|----------|-----|
| 10,000 ideas, 50 changed | 10,000 API calls for children | 50 API calls for children |

---

## Implementation Steps

### 1. Update `read_table_metadata()` (lines 215-243)

Change all tables from snapshot to CDC:

```python
metadata = {
    "ideas": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "idea_proxy_votes": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "idea_comments": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
}
```

### 2. Update `read_table()` to pass `start_offset` (lines 245-256)

Currently ignores `start_offset`. Pass it to all read methods:

```python
def read_table(self, table_name: str, start_offset: dict, table_options: Dict[str, str]):
    if table_name == "ideas":
        return self._read_ideas(start_offset, table_options)
    elif table_name == "idea_proxy_votes":
        return self._read_proxy_votes(start_offset, table_options)
    elif table_name == "idea_comments":
        return self._read_comments(start_offset, table_options)
```

### 3. Update `_get_ideas()` to use `updated_since` (lines 281-310)

Add `updated_since` to the API call when offset is provided:

```python
def _get_ideas(self, start_offset: dict, table_options: Dict[str, str]) -> list[dict]:
    updated_since = start_offset.get("updated_since") if start_offset else None

    extra_params = {}
    if workflow_status:
        extra_params["workflow_status"] = workflow_status
    if q:
        extra_params["q"] = q
    if updated_since:
        extra_params["updated_since"] = updated_since  # NEW

    # Update cache key to include updated_since
    cache_key = (workflow_status, q, max_ideas, updated_since)
    # ... rest of caching logic
```

### 4. Add helper to compute max `updated_at`

```python
def _find_max_updated_at(self, records: list[dict], fallback: str | None = None) -> str | None:
    """Find the maximum updated_at value from records."""
    max_val = fallback
    for record in records:
        updated_at = record.get("updated_at")
        if updated_at and (max_val is None or updated_at > max_val):
            max_val = updated_at
    return max_val
```

### 5. Update `_read_ideas()` (lines 425-442)

Accept offset, return computed next offset:

```python
def _read_ideas(self, start_offset: dict, table_options: Dict[str, str]) -> (Iterator[dict], dict):
    ideas = self._get_ideas(start_offset, table_options)
    # ... existing description flattening ...

    max_updated_at = self._find_max_updated_at(ideas, start_offset.get("updated_since"))
    next_offset = {"updated_since": max_updated_at} if max_updated_at else start_offset or {}
    return iter(ideas), next_offset
```

### 6. Update `_read_proxy_votes()` (lines 444-476)

Accept offset, use filtered ideas, return next offset:

```python
def _read_proxy_votes(self, start_offset: dict, table_options: Dict[str, str]) -> (Iterator[dict], dict):
    ideas = self._get_ideas(start_offset, table_options)  # Now filtered by updated_since
    # ... existing loop to fetch votes per idea ...

    max_updated_at = self._find_max_updated_at(records, start_offset.get("updated_since"))
    next_offset = {"updated_since": max_updated_at} if max_updated_at else start_offset or {}
    return iter(records), next_offset
```

### 7. Update `_read_comments()` (lines 478-510)

Same pattern as proxy_votes.

### 8. Update cache key

Include `updated_since` in the cache key to avoid returning stale cached ideas when offset changes:

```python
cache_key = (workflow_status, q, max_ideas, updated_since)
```

### 9. Update unit tests (`test_aha_unit.py`)

- Change expected `ingestion_type` from `"snapshot"` to `"cdc"`
- Change expected `cursor_field` from `None` to `"updated_at"`
- Add tests for offset handling

### 10. Update integration tests (`test_aha_integration.py`)

- Test first run returns ideas and non-empty offset
- Test second run with offset returns fewer/equal ideas

---

## Files to Modify

1. **`sources/aha/aha.py`** - Main implementation
2. **`sources/aha/test/test_aha_unit.py`** - Update metadata expectations
3. **`sources/aha/test/test_aha_integration.py`** - Add CDC behavior tests

---

## Verification

1. **Unit tests**: `pytest sources/aha/test/test_aha_unit.py -v`
2. **Integration tests**: `pytest sources/aha/test/test_aha_integration.py -v`
3. **Manual verification**:
   - Run connector, note offset returned
   - Run again with same offset, verify fewer API calls (check logs)
   - Add a vote to an old idea, run again, verify that idea appears in results

---

## Design Decisions

### All tables use CDC
Since proxy votes (and likely comments) update the parent idea's `updated_at`, all child tables can safely use incremental filtering. No need for separate full-scan option.

### No delete support
Aha! API doesn't expose deleted ideas. Using `cdc` (not `cdc_with_deletes`) - deleted records remain in downstream tables.

### Return same offset when no records
If no records are returned, return the input `start_offset` unchanged to prevent regression to epoch.

### Cache key includes `updated_since`
Prevents returning stale cached ideas when the offset changes between calls.
