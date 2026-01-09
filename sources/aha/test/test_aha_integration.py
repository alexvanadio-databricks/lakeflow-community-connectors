"""
Integration tests for the Aha! connector.
These tests make actual API calls to the Aha! API.

Run with: pytest sources/aha/test/test_aha_integration.py -v

Requires: sources/aha/configs/dev_config.json with valid credentials:
{
    "api_key": "your_aha_api_key",
    "subdomain": "your_company"
}
"""
import pytest
import sys
from pathlib import Path
import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    ArrayType,
    MapType,
)

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from sources.aha.aha import LakeflowConnect as AhaLakeflowConnect


@pytest.fixture(scope="session")
def integration_options_bounded(integration_config) -> dict:
    """Bounded connector init options for integration tests."""
    # Pull a larger pool of Open ideas so we have a better chance to find ideas with comments,
    # but still keep child-table reads capped to a tiny sample.
    return {**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"}


@pytest.fixture(scope="session")
def live_aha_data(integration_options_bounded):
    """
    Fetch live data once per session to avoid repeated API calls (and rate limiting).
    """
    connector = AhaLakeflowConnect(integration_options_bounded)
    try:
        ideas_iter, _ = connector.read_table("ideas", {}, {})
        ideas = list(ideas_iter)
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Live Aha API not reachable (ideas read failed): {e}")
    except Exception as e:
        # If the environment has no network/DNS, this should be treated as a skip rather than a failure.
        msg = str(e)
        if "NameResolutionError" in msg or "nodename nor servname" in msg:
            pytest.skip(f"Live Aha API not reachable (DNS): {e}")
        raise

    # Prefer ideas that are likely to exercise code paths.
    def _as_int(v):
        try:
            return int(v or 0)
        except Exception:
            return 0

    ideas_with_comments = [i for i in ideas if _as_int(i.get("comments_count")) > 0]
    ideas_with_proxy_votes = [
        i
        for i in ideas
        if _as_int(i.get("endorsements_count")) > 0 or _as_int(i.get("votes")) > 0
    ]

    # Pick small deterministic samples (no randomness).
    comments_sample = ideas_with_comments[:5]
    proxy_votes_sample = ideas_with_proxy_votes[:5] if ideas_with_proxy_votes else ideas[:5]

    # Reorder cached ideas so child-table reads iterate only the selected sample first.
    if connector._ideas_cache is None:
        connector._ideas_cache = {}

    def _reordered(selected: list[dict]) -> list[dict]:
        selected_ids = {x.get("id") for x in selected if isinstance(x, dict)}
        rest = [x for x in ideas if x.get("id") not in selected_ids]
        return list(selected) + rest

    # Fetch proxy votes for the preferred sample.
    proxy_max = min(5, len(proxy_votes_sample))
    proxy_cache_key = ("Open", None, proxy_max)
    try:
        connector._ideas_cache[proxy_cache_key] = _reordered(proxy_votes_sample)
        proxy_iter, _ = connector.read_table(
            "idea_proxy_votes", {}, {"max_ideas": str(proxy_max)}
        )
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Live Aha API not reachable (proxy votes read failed): {e}")
    except Exception as e:
        msg = str(e)
        if "NameResolutionError" in msg or "nodename nor servname" in msg:
            pytest.skip(f"Live Aha API not reachable (DNS): {e}")
        raise

    # Fetch comments for the preferred sample.
    comments_max = min(5, len(comments_sample))
    comments_cache_key = ("Open", None, comments_max)
    try:
        connector._ideas_cache[comments_cache_key] = _reordered(comments_sample)
        comments_iter, _ = connector.read_table(
            "idea_comments", {}, {"max_ideas": str(comments_max)}
        )
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Live Aha API not reachable (comments read failed): {e}")
    except Exception as e:
        msg = str(e)
        if "NameResolutionError" in msg or "nodename nor servname" in msg:
            pytest.skip(f"Live Aha API not reachable (DNS): {e}")
        raise

    return {
        "connector": connector,
        "ideas": ideas,
        "proxy_votes": list(proxy_iter),
        "comments": list(comments_iter),
        "comments_sample_size": len(comments_sample),
    }


def validate_record_against_schema(record: dict, schema: StructType, path: str = "") -> list[str]:
    """
    Validate a record against a PySpark schema.
    Returns a list of validation errors (empty if valid).
    """
    errors = []

    for field in schema.fields:
        field_path = f"{path}.{field.name}" if path else field.name
        value = record.get(field.name)

        # Skip None values - they're allowed for nullable fields
        if value is None:
            continue

        # Validate based on field type
        field_type = field.dataType

        if isinstance(field_type, StringType):
            if not isinstance(value, str):
                errors.append(f"{field_path}: expected string, got {type(value).__name__}")

        elif isinstance(field_type, LongType):
            if not isinstance(value, (int, float)):
                errors.append(f"{field_path}: expected number, got {type(value).__name__}")

        elif isinstance(field_type, BooleanType):
            if not isinstance(value, bool):
                errors.append(f"{field_path}: expected boolean, got {type(value).__name__}")

        elif isinstance(field_type, ArrayType):
            if not isinstance(value, list):
                errors.append(f"{field_path}: expected array, got {type(value).__name__}")
            elif isinstance(field_type.elementType, StructType) and len(value) > 0:
                # Validate first element of array against nested schema
                nested_errors = validate_record_against_schema(
                    value[0], field_type.elementType, f"{field_path}[0]"
                )
                errors.extend(nested_errors)

        elif isinstance(field_type, StructType):
            if not isinstance(value, dict):
                errors.append(f"{field_path}: expected object, got {type(value).__name__}")
            else:
                # Recursively validate nested struct
                nested_errors = validate_record_against_schema(value, field_type, field_path)
                errors.extend(nested_errors)

        elif isinstance(field_type, MapType):
            if not isinstance(value, dict):
                errors.append(f"{field_path}: expected map/object, got {type(value).__name__}")

    return errors


def check_for_unmapped_fields(record: dict, schema: StructType, path: str = "") -> list[str]:
    """
    Check for fields in the record that aren't in the schema.
    Returns a list of unmapped field paths.
    """
    unmapped = []
    schema_field_names = {f.name for f in schema.fields}

    for key, value in record.items():
        field_path = f"{path}.{key}" if path else key

        if key not in schema_field_names:
            unmapped.append(field_path)
        elif value is not None:
            # Check nested structures
            field = next((f for f in schema.fields if f.name == key), None)
            if field and isinstance(field.dataType, StructType) and isinstance(value, dict):
                nested_unmapped = check_for_unmapped_fields(value, field.dataType, field_path)
                unmapped.extend(nested_unmapped)
            elif field and isinstance(field.dataType, ArrayType):
                if isinstance(field.dataType.elementType, StructType) and isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict):
                        nested_unmapped = check_for_unmapped_fields(
                            value[0], field.dataType.elementType, f"{field_path}[0]"
                        )
                        unmapped.extend(nested_unmapped)

    return unmapped


class TestAhaConnectorIntegration:
    """
    Integration tests against live Aha! API.

    Requires sources/aha/configs/dev_config.json with valid credentials.
    """

    def test_list_tables(self, integration_config):
        """Test list_tables against live API."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        tables = connector.list_tables()

        assert isinstance(tables, list)
        assert len(tables) == 3
        assert "ideas" in tables
        assert "idea_proxy_votes" in tables
        assert "idea_comments" in tables

    def test_get_table_schema_all_tables(self, integration_config):
        """Test get_table_schema for all tables against live API."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})

        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            assert schema is not None
            assert len(schema.fields) > 0

    def test_read_table_metadata_all_tables(self, integration_config):
        """Test read_table_metadata for all tables against live API."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})

        for table_name in connector.list_tables():
            metadata = connector.read_table_metadata(table_name, {})
            assert "primary_keys" in metadata
            assert "ingestion_type" in metadata
            assert metadata["ingestion_type"] == "snapshot"

    def test_read_ideas_returns_data(self, live_aha_data):
        """Test that read_table for ideas returns data from live API."""
        records_list = live_aha_data["ideas"]

        # We expect at least some ideas (may be 0 if account is empty)
        assert isinstance(records_list, list)

        if len(records_list) > 0:
            # Verify record structure
            first_record = records_list[0]
            assert "id" in first_record
            assert "reference_num" in first_record or "name" in first_record

    def test_read_proxy_votes_includes_idea_id(self, live_aha_data):
        """Test that proxy votes include idea_id from live API."""
        records_list = live_aha_data["proxy_votes"]

        # Verify each vote has idea_id
        for record in records_list:
            assert "idea_id" in record, "Proxy vote missing idea_id field"
            assert record["idea_id"] is not None, "Proxy vote has null idea_id"

    def test_read_comments_includes_idea_id(self, live_aha_data):
        """Test that comments include idea_id from live API."""
        records_list = live_aha_data["comments"]

        # Verify each comment has idea_id
        for record in records_list:
            assert "idea_id" in record, "Comment missing idea_id field"
            assert record["idea_id"] is not None, "Comment has null idea_id"

    def test_ideas_cache_works_across_tables(self, integration_config, live_aha_data):
        """Test that ideas cache prevents duplicate API calls when using same parameters."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "5"})

        # Read ideas first
        ideas_records, _ = connector.read_table("ideas", {}, {})
        ideas_list = list(ideas_records)

        # Cache should now be populated with key ("Open", None, 5)
        assert connector._ideas_cache is not None
        cache_key = ("Open", None, 5)
        assert cache_key in connector._ideas_cache
        cached_ideas_count = len(connector._ideas_cache[cache_key])

        # Read proxy votes (should use same cached ideas since no table_options override)
        list(connector.read_table("idea_proxy_votes", {}, {})[0])

        # Cache should still have the same entry
        assert cache_key in connector._ideas_cache
        assert len(connector._ideas_cache[cache_key]) == cached_ideas_count

        # Read comments (should use same cached ideas)
        list(connector.read_table("idea_comments", {}, {})[0])

        # Cache should still have the same entry
        assert cache_key in connector._ideas_cache
        assert len(connector._ideas_cache[cache_key]) == cached_ideas_count

    def test_full_connector_workflow(self, live_aha_data):
        """Test a complete workflow: list tables, get schemas, read data."""
        connector = live_aha_data["connector"]

        # 1. List tables
        tables = connector.list_tables()
        assert len(tables) > 0

        # 2. For each table, get schema and metadata
        for table_name in tables:
            schema = connector.get_table_schema(table_name, {})
            assert schema is not None

            metadata = connector.read_table_metadata(table_name, {})
            assert metadata is not None

        # 3. Read data from each table
        for table_name in tables:
            if table_name == "ideas":
                records_list = live_aha_data["ideas"]
            elif table_name == "idea_proxy_votes":
                records_list = live_aha_data["proxy_votes"]
            else:
                records_list = live_aha_data["comments"]
            # Just verify we can iterate through records without error
            assert isinstance(records_list, list)

    def test_api_error_handling(self, integration_config):
        """Test that API errors are properly raised."""
        # Use invalid subdomain to trigger error
        bad_config = {
            "api_key": integration_config["api_key"],
            "subdomain": "this-subdomain-definitely-does-not-exist-12345"
        }
        connector = AhaLakeflowConnect(bad_config)

        with pytest.raises(Exception):
            list(connector.read_table("ideas", {}, {})[0])

    def test_ideas_schema_matches_live_data(self, integration_config, live_aha_data):
        """Validate that live ideas data matches our hardcoded schema."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        schema = connector.get_table_schema("ideas", {})
        records_list = live_aha_data["ideas"]

        if len(records_list) == 0:
            pytest.skip("No ideas in account to validate schema against")

        # Validate each record against schema
        all_errors = []
        for i, record in enumerate(records_list[:5]):  # Check first 5 records
            errors = validate_record_against_schema(record, schema)
            for error in errors:
                all_errors.append(f"Record {i}: {error}")

        assert len(all_errors) == 0, f"Schema validation errors:\n" + "\n".join(all_errors)

    def test_proxy_votes_schema_matches_live_data(self, integration_config, live_aha_data):
        """Validate that live proxy votes data matches our hardcoded schema."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        schema = connector.get_table_schema("idea_proxy_votes", {})
        records_list = live_aha_data["proxy_votes"]

        if len(records_list) == 0:
            pytest.skip("No proxy votes in account to validate schema against")

        # Validate each record against schema
        all_errors = []
        for i, record in enumerate(records_list[:5]):  # Check first 5 records
            errors = validate_record_against_schema(record, schema)
            for error in errors:
                all_errors.append(f"Record {i}: {error}")

        assert len(all_errors) == 0, f"Schema validation errors:\n" + "\n".join(all_errors)

    def test_comments_schema_matches_live_data(self, integration_config, live_aha_data):
        """Validate that live comments data matches our hardcoded schema."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        schema = connector.get_table_schema("idea_comments", {})
        records_list = live_aha_data["comments"]

        if len(records_list) == 0:
            pytest.skip(
                f"No comments found in selected Open ideas sample "
                f"(sample_size={live_aha_data.get('comments_sample_size')})"
            )

        # Validate each record against schema
        all_errors = []
        for i, record in enumerate(records_list[:5]):  # Check first 5 records
            errors = validate_record_against_schema(record, schema)
            for error in errors:
                all_errors.append(f"Record {i}: {error}")

        assert len(all_errors) == 0, f"Schema validation errors:\n" + "\n".join(all_errors)

    def test_ideas_unmapped_fields(self, integration_config, live_aha_data):
        """
        Check for fields returned by the API that aren't in our schema.
        This is a warning-level test - unmapped fields won't cause failures
        but indicate potential data loss.
        """
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        schema = connector.get_table_schema("ideas", {})
        records_list = live_aha_data["ideas"]

        if len(records_list) == 0:
            pytest.skip("No ideas in account to check for unmapped fields")

        # Collect all unmapped fields across records
        all_unmapped = set()
        for record in records_list[:5]:  # Check first 5 records
            unmapped = check_for_unmapped_fields(record, schema)
            all_unmapped.update(unmapped)

        if all_unmapped:
            # This is informational - print but don't fail
            print(f"\n[INFO] Unmapped fields in ideas (not in schema): {sorted(all_unmapped)}")

    def test_proxy_votes_unmapped_fields(self, integration_config, live_aha_data):
        """Check for unmapped fields in proxy votes."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        schema = connector.get_table_schema("idea_proxy_votes", {})
        records_list = live_aha_data["proxy_votes"]

        if len(records_list) == 0:
            pytest.skip("No proxy votes in account to check for unmapped fields")

        all_unmapped = set()
        for record in records_list[:5]:
            unmapped = check_for_unmapped_fields(record, schema)
            all_unmapped.update(unmapped)

        if all_unmapped:
            print(f"\n[INFO] Unmapped fields in proxy_votes (not in schema): {sorted(all_unmapped)}")

    def test_comments_unmapped_fields(self, integration_config, live_aha_data):
        """Check for unmapped fields in comments."""
        connector = AhaLakeflowConnect({**integration_config, "ideas_workflow_status": "Open", "max_ideas": "100"})
        schema = connector.get_table_schema("idea_comments", {})
        records_list = live_aha_data["comments"]

        if len(records_list) == 0:
            pytest.skip(
                f"No comments found in selected Open ideas sample "
                f"(sample_size={live_aha_data.get('comments_sample_size')})"
            )

        all_unmapped = set()
        for record in records_list[:5]:
            unmapped = check_for_unmapped_fields(record, schema)
            all_unmapped.update(unmapped)

        if all_unmapped:
            print(f"\n[INFO] Unmapped fields in comments (not in schema): {sorted(all_unmapped)}")


class TestAhaConnectorWithTestSuite:
    """
    Run the standard LakeflowConnectTester suite against live API.
    """

    def test_with_lakeflow_tester(self, integration_options_bounded, live_aha_data):
        """Run the bounded LakeflowConnectTester suite against live API.

        This is intentionally configured to be as light as our other integration tests:
        - ideas: Open + max_ideas=100
        - child tables: max_ideas=5
        """
        try:
            from tests import test_suite as shared_suite
            from tests.test_suite import LakeflowConnectTester
        except ImportError:
            pytest.skip("LakeflowConnectTester not available")

        # Inject connector into the shared test suite namespace (required by LakeflowConnectTester)
        shared_suite.LakeflowConnect = AhaLakeflowConnect

        # Per-table overrides to keep child-table loops bounded
        table_config = {
            "idea_proxy_votes": {"max_ideas": "5"},
            "idea_comments": {"max_ideas": "5"},
        }

        tester = LakeflowConnectTester(integration_options_bounded, table_config)
        report = tester.run_all_tests()
        tester.print_report(report, show_details=True)

        assert report.passed_tests == report.total_tests
