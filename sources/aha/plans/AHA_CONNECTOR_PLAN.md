# Aha! Proxy Votes and Comments Connector - Implementation Plan

## Overview

Build a Lakeflow community connector for Aha! that ingests ideas, proxy votes, and comments data using snapshot-based synchronization.

## Validated Approach

Your proposed approach is correct:
1. **List all ideas** via `GET /api/v1/ideas`
2. **For each idea**, fetch proxy votes via `GET /api/v1/ideas/:idea_id/endorsements?proxy=true`
3. **For each idea**, fetch comments via `GET /api/v1/ideas/:idea_id/comments`

This is the same parent-child pattern used by the GitHub connector (e.g., reviews depend on pull requests).

**Ingestion Type**: `snapshot` is appropriate - simpler to implement and fits the use case of getting the latest state of all data.

## API Summary

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/ideas` | GET | List all ideas (parent) |
| `/api/v1/ideas/:idea_id/endorsements?proxy=true` | GET | Proxy votes for an idea |
| `/api/v1/ideas/:idea_id/comments` | GET | Comments on an idea |

**Authentication**: Bearer token via `Authorization: Bearer {api_key}` header

**Rate Limits**: 300 requests/min, 20 requests/sec

**Pagination**: Offset-based with `page`, `per_page` params; response includes `total_pages`

## Tables to Implement

### 1. `ideas`
- **Description**: All ideas from Aha!
- **Primary Keys**: `["id"]`
- **Ingestion Type**: `snapshot`
- **Key Fields**: id, reference_num, name, created_at, updated_at, workflow_status, votes, endorsements_count, comments_count, tags, categories

### 2. `idea_proxy_votes`
- **Description**: Proxy votes for all ideas (votes associated with organizations)
- **Primary Keys**: `["id"]`
- **Ingestion Type**: `snapshot`
- **Key Fields**: id, idea_id, created_at, value, weight, endorsed_by_portal_user, endorsed_by_idea_user, idea_organization
- **Parent Dependency**: Requires listing all ideas first, then fetching votes per idea

### 3. `idea_comments`
- **Description**: Comments on all ideas
- **Primary Keys**: `["id"]`
- **Ingestion Type**: `snapshot`
- **Key Fields**: id, idea_id, body, created_at, updated_at, user, attachments
- **Parent Dependency**: Requires listing all ideas first, then fetching comments per idea

## Implementation Steps

### Step 1: Create Directory Structure
```
sources/aha/
  .gitignore                          # Connector-local ignores (e.g., __pycache__, sources/aha/.venv)
  .venv/                              # Optional: local venv stored under sources/aha and ignored by sources/aha/.gitignore
  aha.py                              # Main connector implementation
  test/
    test_aha_unit.py                  # Unit tests with fixtures (run by default)
    test_aha_integration.py           # Integration tests against live API
    fixtures/
      ideas_response.json             # Sanitized real response from /ideas
      proxy_votes_response.json       # Sanitized real response from /endorsements
      comments_response.json          # Sanitized real response from /comments
    conftest.py                       # Pytest fixtures and markers
  configs/
    dev_config.json                   # Test credentials (tracked placeholders; fill locally)
```

### Step 2: Implement `aha.py` with Ideas Caching

```python
class LakeflowConnect:
    def __init__(self, options: dict[str, str]):
        # Required: api_key, subdomain (e.g., "company" for company.aha.io)
        self.api_key = options["api_key"]
        self.subdomain = options["subdomain"]
        # Optional global bounds (used heavily by integration tests)
        self.ideas_workflow_status = options.get("ideas_workflow_status")  # e.g. "Open"
        self.max_ideas = options.get("max_ideas")  # e.g. "200"
        self.base_url = f"https://{self.subdomain}.aha.io/api/v1"
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })
        # Cache ideas keyed by filter (workflow_status/q) so child-table reads reuse the same list.
        self._ideas_cache = None

    def _get_ideas(self) -> list[dict]:
        """Get all ideas, using cache if available."""
        if self._ideas_cache is None:
            self._ideas_cache = self._fetch_paginated("/ideas", "ideas")
        return self._ideas_cache

    def clear_cache(self):
        """Clear the ideas cache (useful between test runs)."""
        self._ideas_cache = None

    def list_tables(self) -> list[str]:
        return ["ideas", "idea_proxy_votes", "idea_comments"]

    def read_table(self, table_name, start_offset, table_options):
        if table_name == "ideas":
            return self._read_ideas()
        elif table_name == "idea_proxy_votes":
            return self._read_proxy_votes()
        elif table_name == "idea_comments":
            return self._read_comments()
```

### Step 3: Implement Reading Methods (Using Cache)

**`_read_ideas()`**:
```python
def _read_ideas(self):
    ideas = self._get_ideas()  # Uses cache
    return iter(ideas), {}
```

**`_read_proxy_votes()`**:
```python
def _read_proxy_votes(self):
    records = []
    ideas = self._get_ideas()  # Uses cache - no duplicate fetch
    for idea in ideas:
        idea_id = idea["id"]
        votes = self._fetch_paginated(f"/ideas/{idea_id}/endorsements?proxy=true", "idea_endorsements")
        for vote in votes:
            vote["idea_id"] = idea_id
            records.append(vote)
    return iter(records), {}
```

**`_read_comments()`**:
```python
def _read_comments(self):
    records = []
    ideas = self._get_ideas()  # Uses cache - no duplicate fetch
    for idea in ideas:
        idea_id = idea["id"]
        comments = self._fetch_paginated(f"/ideas/{idea_id}/comments", "comments")
        for comment in comments:
            comment["idea_id"] = idea_id
            records.append(comment)
    return iter(records), {}
```

### Step 4: Unit Tests with Fixtures

**`test/fixtures/ideas_response.json`** (sanitized real response):
```json
{
  "ideas": [
    {
      "id": "1234567890",
      "reference_num": "PROD-I-1",
      "name": "Sample Feature Request",
      "created_at": "2024-01-15T10:30:00.000Z",
      "updated_at": "2024-01-20T14:45:00.000Z",
      "votes": 5,
      "endorsements_count": 3,
      "comments_count": 2,
      "workflow_status": {"id": "111", "name": "Under review", "complete": false}
    }
  ],
  "pagination": {"total_records": 1, "total_pages": 1, "current_page": 1}
}
```

**`test/conftest.py`**:
```python
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests as integration tests (deselect with '-m not integration')")

@pytest.fixture
def ideas_fixture():
    with open("sources/aha/test/fixtures/ideas_response.json") as f:
        return json.load(f)

@pytest.fixture
def mock_aha_session(ideas_fixture, proxy_votes_fixture, comments_fixture):
    """Mock requests.Session to return fixture data."""
    # Returns a configured mock that responds based on URL patterns
```

**`test/test_aha_unit.py`** (runs by default):
```python
import pytest
from unittest.mock import Mock, patch
from sources.aha.aha import LakeflowConnect

class TestAhaConnectorUnit:
    """Unit tests using fixtures - no network calls."""

    def test_list_tables(self, mock_aha_session):
        connector = LakeflowConnect({"api_key": "fake", "subdomain": "test"})
        tables = connector.list_tables()
        assert tables == ["ideas", "idea_proxy_votes", "idea_comments"]

    def test_read_ideas(self, mock_aha_session, ideas_fixture):
        with patch.object(LakeflowConnect, '_session', mock_aha_session):
            connector = LakeflowConnect({"api_key": "fake", "subdomain": "test"})
            records, offset = connector.read_table("ideas", {}, {})
            records_list = list(records)
            assert len(records_list) == 1
            assert records_list[0]["reference_num"] == "PROD-I-1"

    def test_ideas_cache_prevents_duplicate_fetches(self, mock_aha_session):
        """Verify ideas are only fetched once when reading multiple tables."""
        with patch.object(LakeflowConnect, '_session', mock_aha_session):
            connector = LakeflowConnect({"api_key": "fake", "subdomain": "test"})
            connector.read_table("idea_proxy_votes", {}, {})
            connector.read_table("idea_comments", {}, {})
            # Assert /ideas endpoint was called only once
            ideas_calls = [c for c in mock_aha_session.get.call_args_list if "/ideas" in str(c) and "endorsements" not in str(c)]
            assert len(ideas_calls) == 1

    def test_schema_uses_long_type(self):
        connector = LakeflowConnect({"api_key": "fake", "subdomain": "test"})
        schema = connector.get_table_schema("ideas", {})
        # Verify no IntegerType (should use LongType)
        for field in schema.fields:
            assert "IntegerType" not in str(field.dataType)
```

### Step 5: Live tests (bounded, run by default)

The live suite is designed to be **fast** and **low-risk** for throttling:

- Ideas pool: `ideas_workflow_status="Open"` and `max_ideas=200`
- Child tables (`idea_proxy_votes`, `idea_comments`): cap to **5 ideas** (enough to exercise per-idea loops)
- Connector includes bounded retry/backoff for **HTTP 429**
- If the live API is unreachable (no DNS/network), live tests **skip** rather than fail

**Running tests (current behavior):**

```bash
# Unit + bounded live tests (live tests skip if API is unreachable)
pytest sources/aha/test/ -v
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `sources/aha/aha.py` | Create | Main connector with ideas caching |
| `sources/aha/test/test_aha_unit.py` | Create | Unit tests with mocked fixtures |
| `sources/aha/test/test_aha_integration.py` | Create | Bounded live tests (skip if unreachable) |
| `sources/aha/test/conftest.py` | Create | Pytest fixtures and markers |
| `sources/aha/test/fixtures/ideas_response.json` | Create | Sanitized ideas fixture |
| `sources/aha/test/fixtures/proxy_votes_response.json` | Create | Sanitized proxy votes fixture |
| `sources/aha/test/fixtures/comments_response.json` | Create | Sanitized comments fixture |
| `sources/aha/configs/dev_config.json` | Create | Test credentials (tracked placeholders; fill locally) |

## Configuration Required

```json
{
  "api_key": "your_aha_api_key",
  "subdomain": "your_company"
}
```

## Verification

1. **Unit tests** (default, fast, no network): `pytest sources/aha/test/ -v`
2. **Live tests** (bounded; run by default; skip if unreachable): `pytest sources/aha/test/ -v`
3. **Manual verification**:
   - Confirm ideas are fetched only once when reading proxy_votes and comments
   - Confirm schemas use LongType (not IntegerType)
   - Verify parent-child relationships (idea_id in proxy_votes and comments)

## Key Design Decisions

1. **Ideas Caching**: `_ideas_cache` instance variable prevents duplicate API calls when reading proxy_votes and comments sequentially
2. **Unit Tests First**: Unit tests with fixtures run by default - fast, predictable, no network dependency
3. **Bounded live tests**: Designed to exercise code paths without long runtimes or throttling
4. **Sanitized Fixtures**: Real API responses with sensitive data removed for reproducible testing

## Memory Usage Analysis

### Current Approach: Eager Full Fetch with Caching

The current design fetches ALL ideas into `_ideas_cache` before iterating:

```
Memory usage = O(total_ideas) + O(max(votes_per_idea, comments_per_idea))
```

**Tradeoffs:**
| Aspect | Eager Full Fetch |
|--------|------------------|
| Memory | Holds all ideas in memory simultaneously |
| API calls | Optimal - ideas fetched once, reused |
| Complexity | Simple - straightforward caching |
| Failure mode | If ideas fetch fails, entire operation fails |

**When this is acceptable:**
- Typical Aha! usage: hundreds to low thousands of ideas
- Each idea record is ~1-2KB → 10,000 ideas ≈ 10-20MB memory
- Modern systems handle this easily

**When this becomes problematic:**
- Very large Aha! instances with 100k+ ideas
- Memory-constrained environments
- Long-running processes where memory accumulates

### Alternative: Chunked Processing

For very large datasets, a chunked approach processes ideas in batches:

```python
def _read_proxy_votes_chunked(self):
    """Process ideas in chunks to limit memory usage."""
    records = []
    page = 1
    chunk_size = 100  # Process 100 ideas at a time

    while True:
        # Fetch one page of ideas
        ideas_chunk = self._fetch_page("/ideas", "ideas", page, chunk_size)
        if not ideas_chunk:
            break

        # Process votes for this chunk
        for idea in ideas_chunk:
            votes = self._fetch_paginated(f"/ideas/{idea['id']}/endorsements?proxy=true", "idea_endorsements")
            for vote in votes:
                vote["idea_id"] = idea["id"]
                records.append(vote)

        page += 1

    return iter(records), {}
```

**Tradeoffs:**
| Aspect | Chunked Processing |
|--------|-------------------|
| Memory | O(chunk_size) for ideas - bounded |
| API calls | Higher if both tables need ideas - fetches ideas multiple times |
| Complexity | Higher - pagination state management |
| Failure mode | Partial progress possible, can resume from last chunk |

**Hybrid approach (if needed later):**
- Use chunking for child resources (proxy_votes, comments)
- Each table reads ideas page-by-page, processes children, discards
- Accept the duplicate ideas API calls for memory savings

### Recommendation

**Use eager caching (current plan) because:**
1. Simpler implementation and easier to debug
2. Aha! ideas are typically bounded (few thousand max)
3. Memory impact is modest (~10-20MB for large instances)
4. Optimizes API calls (important with 300 req/min rate limit)

**Consider chunked processing if:**
- Customer reports memory issues with very large Aha! instances
- Connector runs in memory-constrained environments
- Ideas exceed 50k+ records

This is documented here so future maintainers can make an informed decision to refactor if needed.
