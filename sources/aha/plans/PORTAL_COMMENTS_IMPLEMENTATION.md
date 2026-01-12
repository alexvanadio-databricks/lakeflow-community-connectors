# Portal Comments Implementation Plan

## Problem Statement

The Aha! connector was missing portal comments from the ingestion pipeline. Investigation revealed that the Aha! API has **two separate comment systems**:

| Comment Type | API Endpoint | Description |
|--------------|--------------|-------------|
| Internal Comments | `GET /ideas/{id}/comments` | Comments by Aha! users, visible only within the Aha! application |
| Portal Comments | `GET /ideas/{id}/idea_comments` | Comments from ideas portal users, visible in the public/private ideas portal |

The current implementation (`aha.py:527-528`) only fetches internal comments:

```python
comments = self._fetch_paginated(
    f"/ideas/{idea_id}/comments", "comments"
)
```

This means all portal comments (from customers, stakeholders, etc.) are missing from the data pipeline.

---

## API Response Schema Differences

### Internal Comments (`/comments`)
```json
{
  "comments": [
    {
      "id": "123",
      "body": "Comment text",
      "created_at": "2024-01-01T00:00:00.000Z",
      "updated_at": "2024-01-02T00:00:00.000Z",
      "url": "https://...",
      "resource": "...",
      "user": {
        "id": "456",
        "name": "John Doe",
        "email": "john@company.com",
        "created_at": "...",
        "updated_at": "..."
      },
      "attachments": [...]
    }
  ]
}
```

### Portal Comments (`/idea_comments`)
```json
{
  "idea_comments": [
    {
      "id": "789",
      "idea_id": "123",
      "body": "Comment text",
      "created_at": "2024-01-01T00:00:00.000Z",
      "visibility": "Visible to all ideas portal users",
      "parent_idea_comment_id": null,
      "idea_commenter_user": {
        "id": "101",
        "name": "Jane Customer",
        "email": "jane@customer.com"
      },
      "attachments": [...]
    }
  ]
}
```

**Critical difference**: Portal comments have `created_at` but **no `updated_at` field**.

---

## Challenges Explored

### Challenge 1: Different Schemas
The two comment types have fundamentally different response structures:
- Different user object names (`user` vs `idea_commenter_user`)
- Portal comments have `visibility` and `parent_idea_comment_id` (threading)
- Internal comments have `updated_at`; portal comments do not

**Decision**: Create a separate table `idea_portal_comments` rather than merging into the existing `idea_comments` table. This keeps schemas clean and avoids breaking changes.

### Challenge 2: No `updated_at` for Change Detection
The Lakeflow framework supports these ingestion types:

| Type | Description | Requires |
|------|-------------|----------|
| `cdc` | Capture incremental changes (upserts) | `cursor_field` that updates on change |
| `cdc_with_deletes` | CDC + delete detection | `cursor_field` + `read_table_deletes()` |
| `snapshot` | Full data fetch, pipeline reconciles | Nothing (fetches all) |
| `append` | Only new records | `cursor_field` for new records |

Portal comments lack `updated_at`, so:
- **CDC is not viable** - no way to detect edits
- **Snapshot** would work but requires fetching ALL portal comments every run
- **Append** would capture new comments but miss edits/deletes

### Challenge 3: Snapshot Cost vs Accuracy Trade-off

**Snapshot approach**:
- Fetches ALL ideas and ALL their portal comments every pipeline run
- Pipeline uses `apply_changes_from_snapshot` to reconcile (handles inserts, updates, deletes)
- Accurate but expensive in API calls

**The coupling problem**: We explored whether portal comments could use the CDC-filtered ideas list (only fetch comments for recently changed ideas). However:
- `apply_changes_from_snapshot` expects a FULL snapshot
- Providing partial data (only comments from changed ideas) would cause the pipeline to incorrectly mark all other comments as deleted
- The framework doesn't support "scoped snapshot per-parent" patterns

### Challenge 4: Append-Only Limitations

Using `append` with `created_at` as cursor:
- Only captures NEW portal comments
- Edits to existing comments are NOT detected (no `updated_at` to track)
- Deletes are NOT detected

This is a limitation of the Aha! API, not the connector implementation.

---

## Options Considered

### Option A: Full Snapshot
```python
"idea_portal_comments": {
    "primary_keys": ["id"],
    "ingestion_type": "snapshot",
}
```

| Pros | Cons |
|------|------|
| Correct handling of inserts, edits, deletes | High API cost every run |
| Simple implementation | Fetches ALL data regardless of changes |
| No data drift over time | May hit rate limits with large datasets |

### Option B: Append-Only
```python
"idea_portal_comments": {
    "primary_keys": ["id"],
    "cursor_field": "created_at",
    "ingestion_type": "append",
}
```

| Pros | Cons |
|------|------|
| Efficient - only fetches new comments | Misses edits to existing comments |
| Uses CDC-filtered ideas list | Misses deleted comments |
| Low API cost | Data can drift from source over time |

### Option C: Custom Pipeline Logic (Not Implemented)
Theoretically, we could:
1. Fetch CDC-filtered ideas
2. For each changed idea, fetch all its portal comments
3. Custom reconciliation logic at the pipeline layer

This would require changes to the Lakeflow pipeline framework and is out of scope for the connector implementation.

---

## Decision

**Chosen approach: Append-Only (Option B)**

### Rationale
1. **API efficiency** - Only makes API calls for ideas that changed, using the existing CDC-filtered ideas cache
2. **Captures the primary use case** - New portal comments will be ingested
3. **Pragmatic trade-off** - The Aha! API limitation (no `updated_at`) makes perfect accuracy impossible without expensive full snapshots
4. **Non-breaking** - Existing `idea_comments` table continues to work unchanged

### Accepted Limitations
- Edits to portal comments after initial ingestion will NOT be reflected
- Deleted portal comments will remain in the destination table
- For full accuracy, users can periodically run a manual reconciliation or switch to snapshot mode

---

## Implementation Plan

### 1. Add table to `list_tables()` (line 58)
```python
return ["ideas", "idea_proxy_votes", "idea_comments", "idea_portal_comments"]
```

### 2. Add schema for `idea_portal_comments`
```python
"idea_portal_comments": StructType([
    StructField("id", StringType()),
    StructField("idea_id", StringType()),
    StructField("body", StringType()),
    StructField("created_at", StringType()),
    StructField("visibility", StringType()),
    StructField("parent_idea_comment_id", StringType()),
    StructField("idea_commenter_user", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
    ])),
    StructField("attachments", ArrayType(...)),
])
```

### 3. Add metadata
```python
"idea_portal_comments": {
    "primary_keys": ["id"],
    "cursor_field": "created_at",
    "ingestion_type": "append",
}
```

### 4. Add routing in `read_table()`
```python
elif table_name == "idea_portal_comments":
    return self._read_portal_comments(start_offset, table_options)
```

### 5. Implement `_read_portal_comments()` method
- Iterate over CDC-filtered ideas (from `_get_ideas()`)
- Call `/ideas/{idea_id}/idea_comments` endpoint
- Use response key `"idea_comments"`
- Track max `created_at` for append cursor

---

## Verification

1. Run existing tests:
   ```bash
   pytest sources/aha/test/test_aha_lakeflow_connect.py -v
   ```

2. Verify `idea_portal_comments` appears in `list_tables()` output

3. Test against live Aha! instance with portal comments enabled

---

## Future Considerations

If full accuracy becomes critical:
1. Switch `ingestion_type` to `"snapshot"` (accept API cost)
2. Investigate if Aha! API adds `updated_at` to portal comments in the future
3. Consider periodic full-refresh jobs to reconcile drift
