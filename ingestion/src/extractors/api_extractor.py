"""
REST API Extractor.
Handles paginated API ingestion with rate limiting, retries, and backoff.
"""
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, Generator, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import config

logger = logging.getLogger(__name__)


class APIExtractor:
    """
    Extracts data from REST APIs with built-in:
    - Pagination support (offset, cursor, page-based)
    - Rate limiting
    - Exponential backoff retries
    - Response validation
    """

    def __init__(
        self,
        base_url: str = None,
        api_key: str = None,
        rate_limit_per_second: float = 10.0,
        max_retries: int = None,
    ):
        self.base_url = base_url or config.mock_api_base_url
        self.api_key = api_key or config.mock_api_key
        self.rate_limit_per_second = rate_limit_per_second
        self.max_retries = max_retries or config.max_retries
        self._session = requests.Session()
        self._last_request_time = 0

        # Configure session
        self._session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"{config.app_name}/1.0",
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_body: Optional[Dict] = None,
    ) -> Dict:
        """Make an HTTP request with rate limiting and retries."""
        # Rate limiting
        elapsed = time.time() - self._last_request_time
        min_interval = 1.0 / self.rate_limit_per_second
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self._last_request_time = time.time()

        response = self._session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def extract_with_offset_pagination(
        self,
        endpoint: str,
        page_size: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        data_key: str = "data",
        total_key: str = "total",
        max_pages: Optional[int] = None,
    ) -> Generator[List[Dict], None, None]:
        """
        Extract data using offset-based pagination.
        
        Yields batches of records from each page.
        """
        offset = 0
        page = 0

        while True:
            if max_pages and page >= max_pages:
                break

            params = {
                offset_param: offset,
                limit_param: page_size,
            }

            logger.info(f"Fetching page {page + 1}: offset={offset}, limit={page_size}")
            result = self._make_request("GET", endpoint, params=params)

            # Extract data from response
            records = result.get(data_key, [])
            total = result.get(total_key, 0)

            if not records:
                logger.info(f"No more records at offset {offset}")
                break

            yield records
            logger.info(f"Extracted {len(records)} records (total so far: {offset + len(records)}/{total})")

            offset += len(records)
            page += 1

            # Stop if we've reached the end
            if offset >= total:
                break

    def extract_with_cursor_pagination(
        self,
        endpoint: str,
        page_size: int = 100,
        cursor_param: str = "cursor",
        cursor_key: str = "next_cursor",
        data_key: str = "data",
        max_pages: Optional[int] = None,
    ) -> Generator[List[Dict], None, None]:
        """
        Extract data using cursor-based pagination.
        
        Yields batches of records from each page.
        """
        cursor = None
        page = 0

        while True:
            if max_pages and page >= max_pages:
                break

            params = {"limit": page_size}
            if cursor:
                params[cursor_param] = cursor

            logger.info(f"Fetching page {page + 1}: cursor={cursor}")
            result = self._make_request("GET", endpoint, params=params)

            records = result.get(data_key, [])
            cursor = result.get(cursor_key)

            if not records:
                break

            yield records
            page += 1

            if not cursor:
                break

    def extract_single(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Extract a single resource."""
        return self._make_request("GET", endpoint, params=params)

    def extract_with_date_filter(
        self,
        endpoint: str,
        start_date: datetime,
        end_date: datetime,
        date_param: str = "start_date",
        end_date_param: str = "end_date",
        page_size: int = 100,
    ) -> Generator[List[Dict], None, None]:
        """
        Extract data filtered by date range with pagination.
        Useful for incremental loads.
        """
        offset = 0

        while True:
            params = {
                date_param: start_date.isoformat(),
                end_date_param: end_date.isoformat(),
                "offset": offset,
                "limit": page_size,
            }

            result = self._make_request("GET", endpoint, params=params)
            records = result.get("data", [])
            total = result.get("total", 0)

            if not records:
                break

            yield records

            offset += len(records)
            if offset >= total:
                break
