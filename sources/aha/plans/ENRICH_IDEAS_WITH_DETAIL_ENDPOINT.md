# Plan: Enrich Ideas Table with Individual Idea Endpoint Data

## Overview

Update the Aha! connector to fetch additional fields from the single idea endpoint (`GET /ideas/{id}`) after getting the list from the bulk endpoint. This will populate `votes`, `endorsements_count`, `comments_count`, and add other valuable fields.

## Implementation

### 1. Add new method `_enrich_ideas()` in `aha.py`

After fetching ideas via `_fetch_paginated("/ideas", ...)`, iterate through each idea and call `/ideas/{id}` to get full details, then merge the additional fields.

```python
def _enrich_ideas(self, ideas: list[dict]) -> list[dict]:
    """Enrich ideas with additional fields from single idea endpoint."""
    for i, idea in enumerate(ideas):
        idea_id = idea.get("reference_num") or idea.get("id")
        # Log progress
        if (i + 1) % 50 == 0 or i == 0:
            logger.info(f"Enriching idea {i + 1}/{len(ideas)}")

        resp = self._session.get(f"{self.base_url}/ideas/{idea_id}")
        if resp.status_code == 200:
            full_idea = resp.json().get("idea", {})
            # Merge additional fields into the idea
            idea.update({k: full_idea.get(k) for k in ENRICHMENT_FIELDS})
    return ideas
```

### 2. Update `_read_ideas()` to call enrichment

```python
def _read_ideas(self, start_offset: dict, table_options: Dict[str, str]):
    ideas = self._get_ideas(start_offset, table_options)
    ideas = self._enrich_ideas(ideas)  # NEW
    # ... rest of method
```

### 3. Update ideas schema

**Keep existing fields:**
- id, reference_num, name, description, created_at, updated_at, url, workflow_status

**Fields now populated (already in schema):**
- votes (LongType)
- endorsements_count (LongType)
- comments_count (LongType)

**New simple fields to add:**
- score (LongType)
- initial_votes (LongType)
- status_changed_at (StringType)
- visibility (StringType)
- product_id (StringType)
- submitted_idea_portal_record_url (StringType)

**New nested fields to add:**
- product: {id, reference_prefix, name, product_line, created_at, workspace_type, url}
- created_by_portal_user: {id, name, email, created_at}
- created_by_idea_user: {id, name, email, created_at, title}
- assigned_to_user: {id, name, email} (nullable)
- categories: Array of {id, name, parent_id, project_id, created_at}
- tags: Array of StringType

**Fields to SKIP (complex/low value):**
- score_facts - complex scoring breakdown, often empty
- full_tags - redundant with tags, more complex structure
- custom_fields - varies by account, complex schema
- workflow_status_times - status history array, complex
- integration_fields - external integrations, often empty

### 4. Update unit tests

- Update `test_get_table_schema_ideas()` to validate new fields
- Update fixture `ideas_response.json` with enriched data
- Add test for `_enrich_ideas()` method

### 5. Handle rate limiting

The existing `_fetch_paginated` has retry logic for 429s. Apply same pattern to enrichment calls.

## Files to Modify

- `sources/aha/aha.py` - Add enrichment method, update schema, update `_read_ideas()`
- `sources/aha/test/test_aha_unit.py` - Update schema assertions
- `sources/aha/test/fixtures/ideas_response.json` - Add enriched fields to fixture

## Performance Consideration

For 50 ideas (current max_ideas setting), this adds 50 additional API calls. With rate limiting and typical API latency, expect ~1-2 minutes for enrichment. This is acceptable for CDC workloads where incremental updates are small.

## Verification

```bash
pytest sources/aha/test/ -v
```

Then manually verify ideas table has populated votes/endorsements_count/comments_count fields.
