from typing import Iterator, Dict, List, Optional, Tuple
import logging
import requests
from urllib.parse import quote_plus
import time
import re
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    ArrayType,
    MapType,
)

logger = logging.getLogger(__name__)


class LakeflowConnect:
    """
    Aha! connector for Lakeflow Community Connectors.

    Ingests ideas, proxy votes, and comments from the Aha! API.
    Uses CDC (Change Data Capture) synchronization with updated_since filtering.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Aha! connector.

        Args:
            options: Dictionary containing:
                - api_key: Aha! API key (Bearer token)
                - subdomain: Aha! subdomain (e.g., "company" for company.aha.io)
        """
        self.api_key = options["api_key"]
        self.subdomain = options["subdomain"]
        # Optional global filters/safeguards
        self.ideas_workflow_status: Optional[str] = options.get("ideas_workflow_status")
        self.max_ideas: Optional[int] = self._parse_optional_int(options.get("max_ideas"))
        self.base_url = f"https://{self.subdomain}.aha.io/api/v1"
        
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        # Cache for ideas list, keyed by (workflow_status, q, max_ideas, updated_since).
        # Keep None as the "cleared" state for compatibility with existing tests.
        self._ideas_cache: Optional[dict[Tuple[Optional[str], Optional[str], Optional[int], Optional[str]], list[dict]]] = None

    def list_tables(self) -> List[str]:
        """Return the list of available Aha! tables."""
        return ["ideas", "idea_proxy_votes", "idea_comments"]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        schemas = {
            "ideas": StructType(
                [
                    StructField("id", StringType()),
                    StructField("reference_num", StringType()),
                    StructField("name", StringType()),
                    StructField("description", StringType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("votes", LongType()),
                    StructField("endorsements_count", LongType()),
                    StructField("comments_count", LongType()),
                    StructField("url", StringType()),
                    StructField(
                        "workflow_status",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("name", StringType()),
                                StructField("complete", BooleanType()),
                            ]
                        ),
                    ),
                ]
            ),
            "idea_proxy_votes": StructType(
                [
                    StructField("id", StringType()),
                    StructField("idea_id", StringType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("value", LongType()),
                    StructField("link", StringType()),
                    StructField("weight", LongType()),
                    StructField(
                        "endorsed_by_portal_user",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("name", StringType()),
                                StructField("email", StringType()),
                                StructField("created_at", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "endorsed_by_idea_user",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("name", StringType()),
                                StructField("email", StringType()),
                                StructField("created_at", StringType()),
                                StructField("title", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "idea_organization",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("name", StringType()),
                                StructField("created_at", StringType()),
                                StructField("url", StringType()),
                                StructField("resource", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "idea_users",
                        ArrayType(
                            StructType(
                                [
                                    StructField("id", StringType()),
                                    StructField("first_name", StringType()),
                                    StructField("last_name", StringType()),
                                    StructField("email", StringType()),
                                ]
                            )
                        ),
                    ),
                    StructField(
                        "description",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("body", StringType()),
                                StructField("editor_version", LongType()),
                                StructField("created_at", StringType()),
                                StructField("updated_at", StringType()),
                                StructField(
                                    "attachments",
                                    ArrayType(MapType(StringType(), StringType())),
                                ),
                            ]
                        ),
                    ),
                    StructField(
                        "custom_fields",
                        ArrayType(MapType(StringType(), StringType())),
                    ),
                ]
            ),
            "idea_comments": StructType(
                [
                    StructField("id", StringType()),
                    StructField("idea_id", StringType()),
                    StructField("body", StringType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("url", StringType()),
                    StructField("resource", StringType()),
                    StructField(
                        "user",
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("name", StringType()),
                                StructField("email", StringType()),
                                StructField("created_at", StringType()),
                                StructField("updated_at", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "attachments",
                        ArrayType(
                            StructType(
                                [
                                    StructField("id", StringType()),
                                    StructField("download_url", StringType()),
                                    StructField("created_at", StringType()),
                                    StructField("file_name", StringType()),
                                    StructField("file_size", LongType()),
                                    StructField("content_type", StringType()),
                                ]
                            )
                        ),
                    ),
                ]
            ),
        }

        if table_name not in schemas:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return schemas[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        All tables use CDC ingestion type with updated_at as cursor field.
        """
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

        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read data from the specified Aha! table with CDC support."""
        if table_name == "ideas":
            return self._read_ideas(start_offset, table_options)
        elif table_name == "idea_proxy_votes":
            return self._read_proxy_votes(start_offset, table_options)
        elif table_name == "idea_comments":
            return self._read_comments(start_offset, table_options)
        else:
            raise ValueError(f"Table '{table_name}' is not supported.")

    @staticmethod
    def _parse_optional_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        s = str(value).strip()
        if not s:
            return None
        return int(s)

    def _effective_ideas_workflow_status(self, table_options: Dict[str, str]) -> Optional[str]:
        # Per-table override, else connector default.
        return table_options.get("ideas_workflow_status") or self.ideas_workflow_status

    def _effective_max_ideas(self, table_options: Dict[str, str]) -> Optional[int]:
        override = self._parse_optional_int(table_options.get("max_ideas"))
        return override if override is not None else self.max_ideas

    def _effective_ideas_q(self, table_options: Dict[str, str]) -> Optional[str]:
        # Support q as a convenience alias for ideas table.
        return table_options.get("ideas_q") or table_options.get("q")

    def _get_ideas(self, start_offset: dict, table_options: Dict[str, str]) -> list[dict]:
        """
        Get ideas, using cache if available.
        Supports CDC filtering via updated_since from start_offset.
        This prevents duplicate API calls when reading child tables.
        """
        workflow_status = self._effective_ideas_workflow_status(table_options)
        q = self._effective_ideas_q(table_options)
        max_ideas = self._effective_max_ideas(table_options)
        updated_since = start_offset.get("updated_since") if start_offset else None
        cache_key = (workflow_status, q, max_ideas, updated_since)

        if self._ideas_cache is None:
            self._ideas_cache = {}

        if cache_key not in self._ideas_cache:
            extra_params: dict[str, str] = {}
            if workflow_status:
                extra_params["workflow_status"] = workflow_status
            if q:
                # Aha docs: q searches against idea name; pass-through.
                extra_params["q"] = q
            if updated_since:
                # CDC: only fetch ideas updated since the last checkpoint
                extra_params["updated_since"] = updated_since

            # Fetch at most max_ideas ideas (early stop) to bound work.
            self._ideas_cache[cache_key] = self._fetch_paginated(
                "/ideas",
                "ideas",
                extra_params=extra_params if extra_params else None,
                max_records=max_ideas,
            )

        return self._ideas_cache[cache_key]

    def clear_cache(self) -> None:
        """Clear the ideas cache. Useful between test runs."""
        self._ideas_cache = None

    @staticmethod
    def _find_max_updated_at(records: list[dict], fallback: Optional[str] = None) -> Optional[str]:
        """
        Find the maximum updated_at value from records.
        Used to compute the next offset for CDC.
        """
        max_val = fallback
        for record in records:
            updated_at = record.get("updated_at")
            if updated_at and (max_val is None or updated_at > max_val):
                max_val = updated_at
        return max_val

    def _fetch_paginated(
        self,
        endpoint: str,
        response_key: str,
        extra_params: dict | None = None,
        max_records: Optional[int] = None,
    ) -> list[dict]:
        """
        Generic pagination handler for Aha! API.

        Args:
            endpoint: API endpoint (e.g., "/ideas")
            response_key: Key in response JSON containing the records
            extra_params: Additional query parameters

        Returns:
            List of all records across all pages
        """
        records = []
        page = 1
        per_page = 100

        while True:
            # Build URL - handle endpoints that already have query params
            if "?" in endpoint:
                url = f"{self.base_url}{endpoint}&page={page}&per_page={per_page}"
            else:
                url = f"{self.base_url}{endpoint}?page={page}&per_page={per_page}"

            # Add extra params if provided
            if extra_params:
                for key, value in extra_params.items():
                    url += f"&{key}={quote_plus(str(value))}"

            resp = None
            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                resp = self._session.get(url)

                # Handle vendor throttling with a bounded retry/backoff.
                if resp.status_code == 429 and attempt < max_attempts:
                    sleep_s = self._throttle_sleep_seconds(resp)
                    logger.warning(
                        "Aha API throttled (429) for %s; sleeping %ss then retrying (attempt %d/%d)",
                        endpoint,
                        sleep_s,
                        attempt,
                        max_attempts,
                    )
                    time.sleep(sleep_s)
                    continue

                break

            assert resp is not None
            if resp.status_code != 200:
                logger.error(f"API error for {endpoint}: {resp.status_code} {resp.text}")
                raise Exception(
                    f"Aha! API error for {endpoint}: {resp.status_code} {resp.text}"
                )

            data = resp.json()
            page_records = data.get(response_key, [])
            records.extend(page_records)

            # Early stop if we reached max_records
            if max_records is not None and len(records) >= max_records:
                break

            # Check pagination
            pagination = data.get("pagination", {})
            total_pages = pagination.get("total_pages", 1)

            if page >= total_pages:
                break

            page += 1

        return records

    @staticmethod
    def _throttle_sleep_seconds(resp) -> int:
        """
        Choose a conservative sleep duration for 429s.
        - Prefer Retry-After header when present.
        - Otherwise parse Aha's message like 'Try again in 60 seconds.'
        - Fallback to 60.
        """
        retry_after = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
        if retry_after:
            try:
                return max(1, int(float(retry_after)))
            except Exception:
                pass

        try:
            data = resp.json()
            errors = data.get("errors") if isinstance(data, dict) else None
            if isinstance(errors, list) and errors:
                msg = errors[0].get("message")
                if isinstance(msg, str):
                    m = re.search(r"(\d+)\s*seconds", msg)
                    if m:
                        return max(1, int(m.group(1)))
        except Exception:
            pass

        return 60

    def _read_ideas(self, start_offset: dict, table_options: Dict[str, str]) -> (Iterator[dict], dict):
        """Read ideas from Aha! with CDC support."""
        updated_since = start_offset.get("updated_since") if start_offset else None
        logger.info(f"Fetching ideas from Aha! (updated_since={updated_since})")
        ideas = self._get_ideas(start_offset, table_options)
        logger.info(f"Fetched {len(ideas)} ideas")
        # Flatten description.body -> description
        for idea in ideas:
            desc = idea.get("description")
            if isinstance(desc, dict):
                idea["description"] = desc.get("body")
            else:
                idea["description"] = None
                if desc is not None:
                    logger.warning(
                        f"Could not extract description for idea {idea.get('id')}: "
                        f"expected dict, got {type(desc).__name__}"
                    )
        # Compute next offset from max updated_at
        max_updated_at = self._find_max_updated_at(ideas, updated_since)
        next_offset = {"updated_since": max_updated_at} if max_updated_at else (start_offset or {})
        return iter(ideas), next_offset

    def _read_proxy_votes(self, start_offset: dict, table_options: Dict[str, str]) -> (Iterator[dict], dict):
        """
        Read proxy votes for ideas with CDC support.
        Uses cached ideas list to avoid duplicate API calls.
        """
        updated_since = start_offset.get("updated_since") if start_offset else None
        records = []
        ideas = self._get_ideas(start_offset, table_options)
        total_ideas = len(ideas)

        logger.info(f"Fetching proxy votes for {total_ideas} ideas (updated_since={updated_since})")

        for i, idea in enumerate(ideas):
            idea_id = idea.get("id")
            if not idea_id:
                logger.warning(f"Idea at index {i} has no id, skipping")
                continue

            # Log progress every 50 ideas
            if (i + 1) % 50 == 0 or i == 0:
                logger.info(f"Processing idea {i + 1}/{total_ideas} for proxy votes")

            # Fetch proxy votes for this idea
            votes = self._fetch_paginated(
                f"/ideas/{idea_id}/endorsements?proxy=true", "idea_endorsements"
            )

            for vote in votes:
                # Ensure idea_id is present in each vote record
                vote["idea_id"] = idea_id
                records.append(vote)

        logger.info(f"Fetched {len(records)} proxy votes across {total_ideas} ideas")
        # Compute next offset from max updated_at
        max_updated_at = self._find_max_updated_at(records, updated_since)
        next_offset = {"updated_since": max_updated_at} if max_updated_at else (start_offset or {})
        return iter(records), next_offset

    def _read_comments(self, start_offset: dict, table_options: Dict[str, str]) -> (Iterator[dict], dict):
        """
        Read comments for ideas with CDC support.
        Uses cached ideas list to avoid duplicate API calls.
        """
        updated_since = start_offset.get("updated_since") if start_offset else None
        records = []
        ideas = self._get_ideas(start_offset, table_options)
        total_ideas = len(ideas)

        logger.info(f"Fetching comments for {total_ideas} ideas (updated_since={updated_since})")

        for i, idea in enumerate(ideas):
            idea_id = idea.get("id")
            if not idea_id:
                logger.warning(f"Idea at index {i} has no id, skipping")
                continue

            # Log progress every 50 ideas
            if (i + 1) % 50 == 0 or i == 0:
                logger.info(f"Processing idea {i + 1}/{total_ideas} for comments")

            # Fetch comments for this idea
            comments = self._fetch_paginated(
                f"/ideas/{idea_id}/comments", "comments"
            )

            for comment in comments:
                # Ensure idea_id is present in each comment record
                comment["idea_id"] = idea_id
                records.append(comment)

        logger.info(f"Fetched {len(records)} comments across {total_ideas} ideas")
        # Compute next offset from max updated_at
        max_updated_at = self._find_max_updated_at(records, updated_since)
        next_offset = {"updated_since": max_updated_at} if max_updated_at else (start_offset or {})
        return iter(records), next_offset
