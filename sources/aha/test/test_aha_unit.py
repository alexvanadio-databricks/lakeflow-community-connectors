"""
Unit tests for the Aha! connector.
These tests use fixtures and mocks - no network calls are made.

Run with: pytest sources/aha/test/test_aha_unit.py -v
"""
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, LongType, IntegerType

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from sources.aha.aha import LakeflowConnect as AhaLakeflowConnect


class TestAhaConnectorUnit:
    """Unit tests using fixtures - no network calls."""

    def test_list_tables(self):
        """Test that list_tables returns the expected table names."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        tables = connector.list_tables()
        assert tables == ["ideas", "idea_proxy_votes", "idea_comments"]

    def test_list_tables_order(self):
        """Test that list_tables returns tables in consistent order."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        tables1 = connector.list_tables()
        tables2 = connector.list_tables()
        assert tables1 == tables2

    def test_get_table_schema_ideas(self):
        """Test that get_table_schema returns valid schema for ideas."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        schema = connector.get_table_schema("ideas", {})
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0

        # Check for expected fields
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "reference_num" in field_names
        assert "name" in field_names
        assert "created_at" in field_names
        assert "workflow_status" in field_names

    def test_get_table_schema_proxy_votes(self):
        """Test that get_table_schema returns valid schema for proxy votes."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        schema = connector.get_table_schema("idea_proxy_votes", {})
        assert isinstance(schema, StructType)

        # Check for expected fields including the parent reference
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "idea_id" in field_names
        assert "value" in field_names
        assert "weight" in field_names
        assert "idea_organization" in field_names

    def test_get_table_schema_comments(self):
        """Test that get_table_schema returns valid schema for comments."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        schema = connector.get_table_schema("idea_comments", {})
        assert isinstance(schema, StructType)

        # Check for expected fields including the parent reference
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "idea_id" in field_names
        assert "body" in field_names
        assert "user" in field_names

    def test_get_table_schema_invalid_table(self):
        """Test that get_table_schema raises error for invalid table."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        with pytest.raises(ValueError, match="not supported"):
            connector.get_table_schema("invalid_table", {})

    def test_schema_uses_long_type_not_integer_type(self):
        """
        Verify all schemas use LongType instead of IntegerType.
        This is a requirement of the LakeflowConnect interface.
        """
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})

        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            _check_no_integer_type(schema, table_name)

    def test_read_table_metadata_ideas(self):
        """Test metadata for ideas table."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        metadata = connector.read_table_metadata("ideas", {})

        assert "primary_keys" in metadata
        assert "ingestion_type" in metadata
        assert metadata["primary_keys"] == ["id"]
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_all_tables_are_snapshot(self):
        """Test that all tables use snapshot ingestion type."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})

        for table_name in connector.list_tables():
            metadata = connector.read_table_metadata(table_name, {})
            assert metadata["ingestion_type"] == "snapshot", (
                f"Table {table_name} should use snapshot ingestion"
            )

    def test_read_table_metadata_invalid_table(self):
        """Test that read_table_metadata raises error for invalid table."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        with pytest.raises(ValueError, match="not supported"):
            connector.read_table_metadata("invalid_table", {})

    def test_read_ideas(self, mock_aha_session, ideas_fixture):
        """Test reading ideas table with mocked session."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        connector._session = mock_aha_session

        records, offset = connector.read_table("ideas", {}, {})
        records_list = list(records)

        assert len(records_list) == 2
        assert records_list[0]["reference_num"] == "PROD-I-1"
        assert records_list[1]["reference_num"] == "PROD-I-2"
        assert offset == {}

    def test_read_proxy_votes(self, mock_aha_session, ideas_fixture, proxy_votes_fixture):
        """Test reading proxy votes table with mocked session."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        connector._session = mock_aha_session

        records, offset = connector.read_table("idea_proxy_votes", {}, {})
        records_list = list(records)

        # Should have votes for each idea (2 ideas x 2 votes each = 4 total in mock)
        assert len(records_list) > 0

        # Each vote should have idea_id populated
        for record in records_list:
            assert "idea_id" in record
            assert record["idea_id"] is not None

    def test_read_comments(self, mock_aha_session, ideas_fixture, comments_fixture):
        """Test reading comments table with mocked session."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        connector._session = mock_aha_session

        records, offset = connector.read_table("idea_comments", {}, {})
        records_list = list(records)

        # Should have comments for each idea
        assert len(records_list) > 0

        # Each comment should have idea_id populated
        for record in records_list:
            assert "idea_id" in record
            assert record["idea_id"] is not None

    def test_read_invalid_table(self, mock_aha_session):
        """Test that read_table raises error for invalid table."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        connector._session = mock_aha_session

        with pytest.raises(ValueError, match="not supported"):
            connector.read_table("invalid_table", {}, {})

    def test_ideas_cache_prevents_duplicate_fetches(self, mock_aha_session):
        """
        Verify ideas are only fetched once when reading multiple tables.
        This tests the caching behavior.
        """
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        connector._session = mock_aha_session

        # Track calls to the session
        call_count = {"ideas": 0, "endorsements": 0, "comments": 0}

        original_get = mock_aha_session.get

        def tracking_get(url, **kwargs):
            if "/endorsements" in url:
                call_count["endorsements"] += 1
            elif "/comments" in url:
                call_count["comments"] += 1
            elif "/ideas" in url and "/endorsements" not in url and "/comments" not in url:
                call_count["ideas"] += 1
            return original_get(url, **kwargs)

        mock_aha_session.get = tracking_get

        # Read proxy votes (should fetch ideas first)
        list(connector.read_table("idea_proxy_votes", {}, {})[0])

        # Read comments (should reuse cached ideas)
        list(connector.read_table("idea_comments", {}, {})[0])

        # Ideas endpoint should only be called once due to caching
        assert call_count["ideas"] == 1, (
            f"Ideas endpoint called {call_count['ideas']} times, expected 1"
        )

    def test_clear_cache(self, mock_aha_session):
        """Test that clear_cache resets the ideas cache."""
        connector = AhaLakeflowConnect({"api_key": "fake", "subdomain": "test"})
        connector._session = mock_aha_session

        # Populate cache
        list(connector.read_table("ideas", {}, {})[0])
        assert connector._ideas_cache is not None

        # Clear cache
        connector.clear_cache()
        assert connector._ideas_cache is None

    def test_connector_initialization(self):
        """Test connector initialization with required options."""
        connector = AhaLakeflowConnect({
            "api_key": "test-api-key",
            "subdomain": "mycompany"
        })

        assert connector.api_key == "test-api-key"
        assert connector.subdomain == "mycompany"
        assert connector.base_url == "https://mycompany.aha.io/api/v1"

    def test_connector_initialization_missing_api_key(self):
        """Test that connector raises error when api_key is missing."""
        with pytest.raises(KeyError):
            AhaLakeflowConnect({"subdomain": "test"})

    def test_connector_initialization_missing_subdomain(self):
        """Test that connector raises error when subdomain is missing."""
        with pytest.raises(KeyError):
            AhaLakeflowConnect({"api_key": "test"})


def _check_no_integer_type(schema: StructType, table_name: str, path: str = ""):
    """
    Recursively check that a schema doesn't contain IntegerType.
    All integer fields should use LongType.
    """
    for field in schema.fields:
        field_path = f"{path}.{field.name}" if path else field.name

        if isinstance(field.dataType, IntegerType):
            pytest.fail(
                f"Table '{table_name}' field '{field_path}' uses IntegerType. "
                "Use LongType instead."
            )

        # Recursively check nested StructTypes
        if isinstance(field.dataType, StructType):
            _check_no_integer_type(field.dataType, table_name, field_path)
