"""
Pytest configuration and fixtures for Aha! connector tests.
"""
import json
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock
import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (deselect with '-m not integration')",
    )


# Get the directory containing fixtures
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def ideas_fixture():
    """Load the ideas fixture data."""
    with open(FIXTURES_DIR / "ideas_response.json") as f:
        return json.load(f)


@pytest.fixture
def proxy_votes_fixture():
    """Load the proxy votes fixture data."""
    with open(FIXTURES_DIR / "proxy_votes_response.json") as f:
        return json.load(f)


@pytest.fixture
def comments_fixture():
    """Load the comments fixture data."""
    with open(FIXTURES_DIR / "comments_response.json") as f:
        return json.load(f)


@pytest.fixture
def mock_aha_session(ideas_fixture, proxy_votes_fixture, comments_fixture):
    """
    Create a mock requests.Session that returns fixture data based on URL patterns.
    """
    mock_session = MagicMock()

    def mock_get(url, **kwargs):
        """Route requests to appropriate fixture data."""
        response = Mock()
        response.status_code = 200

        # Determine which fixture to return based on URL
        if "/endorsements" in url:
            response.json.return_value = proxy_votes_fixture
        elif "/comments" in url:
            response.json.return_value = comments_fixture
        elif "/ideas" in url:
            response.json.return_value = ideas_fixture
        else:
            response.status_code = 404
            response.json.return_value = {"error": "Not found"}

        return response

    mock_session.get = mock_get
    mock_session.headers = MagicMock()
    mock_session.headers.update = MagicMock()

    return mock_session


@pytest.fixture(scope="session")
def integration_config():
    """
    Load integration test configuration from dev_config.json.
    Skips the test if config file doesn't exist.
    """
    config_path = Path(__file__).parent.parent / "configs" / "dev_config.json"
    if not config_path.exists():
        pytest.skip(
            f"Integration test config not found at {config_path}. "
            "Create this file with your Aha! API credentials to run integration tests."
        )

    with open(config_path) as f:
        return json.load(f)
