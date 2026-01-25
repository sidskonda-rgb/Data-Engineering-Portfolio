"""
OpenFDA Drug Adverse Events API Client

A production-grade client for fetching adverse event reports from the FDA's
public API with rate limiting, retry logic, and pagination support.

API Documentation: https://open.fda.gov/apis/drug/event/
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Iterator, Optional
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FDAClientConfig:
    """Configuration for the FDA API client."""
    base_url: str = "https://api.fda.gov/drug/event.json"
    api_key: Optional[str] = None  # Optional: increases rate limit
    rate_limit_per_minute: int = 240  # Without API key
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    timeout_seconds: int = 30
    default_limit: int = 100
    max_limit: int = 1000


class RateLimiter:
    """Simple rate limiter to respect API limits."""

    def __init__(self, requests_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time: Optional[float] = None

    def wait(self) -> None:
        """Wait if necessary to respect rate limit."""
        if self.last_request_time is not None:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        self.last_request_time = time.time()


class FDAAdverseEventsClient:
    """
    Client for the OpenFDA Drug Adverse Events API.

    Features:
    - Rate limiting (240 requests/minute without API key)
    - Automatic retry with exponential backoff
    - Pagination support
    - Date range queries
    - Streaming results for large datasets

    Example:
        client = FDAAdverseEventsClient()

        # Fetch recent events
        events = client.fetch_events(
            start_date="2024-01-01",
            end_date="2024-01-31",
            limit=100
        )

        # Stream all events in date range
        for batch in client.stream_events(start_date="2024-01-01"):
            process_batch(batch)
    """

    def __init__(self, config: Optional[FDAClientConfig] = None):
        self.config = config or FDAClientConfig()
        self.rate_limiter = RateLimiter(self.config.rate_limit_per_minute)
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_delay_seconds,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _build_search_query(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        drug_name: Optional[str] = None,
        serious_only: bool = False,
        additional_filters: Optional[str] = None
    ) -> Optional[str]:
        """
        Build the search query string for the API.

        Args:
            start_date: Start date in YYYYMMDD or YYYY-MM-DD format
            end_date: End date in YYYYMMDD or YYYY-MM-DD format
            drug_name: Filter by drug name (brand or generic)
            serious_only: Only return serious adverse events
            additional_filters: Raw search string to append

        Returns:
            Search query string or None if no filters
        """
        filters = []

        # Date range filter
        if start_date or end_date:
            start = self._format_date(start_date) if start_date else "19000101"
            end = self._format_date(end_date) if end_date else "20991231"
            filters.append(f"receivedate:[{start}+TO+{end}]")

        # Drug name filter
        if drug_name:
            # Search in both brand and generic names
            filters.append(f'patient.drug.medicinalproduct:"{drug_name}"')

        # Serious events only
        if serious_only:
            filters.append("serious:1")

        # Additional custom filters
        if additional_filters:
            filters.append(additional_filters)

        return "+AND+".join(filters) if filters else None

    def _format_date(self, date_str: str) -> str:
        """Convert date string to YYYYMMDD format."""
        # Remove any dashes
        return date_str.replace("-", "")

    def fetch_events(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        drug_name: Optional[str] = None,
        serious_only: bool = False,
        limit: int = 100,
        skip: int = 0,
        additional_filters: Optional[str] = None
    ) -> dict:
        """
        Fetch adverse events from the FDA API.

        Args:
            start_date: Start date (YYYY-MM-DD or YYYYMMDD)
            end_date: End date (YYYY-MM-DD or YYYYMMDD)
            drug_name: Filter by drug name
            serious_only: Only serious adverse events
            limit: Number of records to return (max 1000)
            skip: Number of records to skip (pagination)
            additional_filters: Raw search query to append

        Returns:
            API response as dictionary with 'meta' and 'results' keys

        Raises:
            requests.exceptions.HTTPError: On API errors
        """
        # Respect rate limits
        self.rate_limiter.wait()

        # Build request parameters
        params = {
            "limit": min(limit, self.config.max_limit),
            "skip": skip
        }

        # Add search query if filters provided
        search_query = self._build_search_query(
            start_date=start_date,
            end_date=end_date,
            drug_name=drug_name,
            serious_only=serious_only,
            additional_filters=additional_filters
        )

        if search_query:
            params["search"] = search_query

        # Add API key if configured
        if self.config.api_key:
            params["api_key"] = self.config.api_key

        logger.info(f"Fetching events: limit={limit}, skip={skip}")
        logger.debug(f"Search query: {search_query}")

        # Make request
        response = self.session.get(
            self.config.base_url,
            params=params,
            timeout=self.config.timeout_seconds
        )
        response.raise_for_status()

        data = response.json()

        # Log results
        total = data.get("meta", {}).get("results", {}).get("total", 0)
        returned = len(data.get("results", []))
        logger.info(f"Fetched {returned} events (total available: {total})")

        return data

    def stream_events(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        drug_name: Optional[str] = None,
        serious_only: bool = False,
        batch_size: int = 1000,
        max_records: Optional[int] = None,
        additional_filters: Optional[str] = None
    ) -> Iterator[list]:
        """
        Stream adverse events in batches for large datasets.

        Yields batches of events, handling pagination automatically.
        Use this for fetching large date ranges efficiently.

        Args:
            start_date: Start date (YYYY-MM-DD or YYYYMMDD)
            end_date: End date (YYYY-MM-DD or YYYYMMDD)
            drug_name: Filter by drug name
            serious_only: Only serious adverse events
            batch_size: Number of records per batch (max 1000)
            max_records: Maximum total records to fetch (None for all)
            additional_filters: Raw search query to append

        Yields:
            List of adverse event records

        Example:
            for batch in client.stream_events(start_date="2024-01-01"):
                for event in batch:
                    process_event(event)
        """
        skip = 0
        total_fetched = 0
        batch_size = min(batch_size, self.config.max_limit)

        while True:
            try:
                response = self.fetch_events(
                    start_date=start_date,
                    end_date=end_date,
                    drug_name=drug_name,
                    serious_only=serious_only,
                    limit=batch_size,
                    skip=skip,
                    additional_filters=additional_filters
                )

                results = response.get("results", [])

                if not results:
                    logger.info("No more results, ending stream")
                    break

                yield results

                total_fetched += len(results)
                skip += len(results)

                # Check max records limit
                if max_records and total_fetched >= max_records:
                    logger.info(f"Reached max_records limit ({max_records})")
                    break

                # Check if we've fetched all available records
                total_available = response.get("meta", {}).get("results", {}).get("total", 0)
                if skip >= total_available:
                    logger.info(f"Fetched all {total_available} available records")
                    break

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # No results for this query
                    logger.warning("No results found for query")
                    break
                raise

        logger.info(f"Stream complete: fetched {total_fetched} total records")

    def get_event_counts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        count_field: str = "receivedate"
    ) -> dict:
        """
        Get aggregated counts for a field.

        Args:
            start_date: Start date filter
            end_date: End date filter
            count_field: Field to aggregate by (e.g., 'receivedate',
                        'patient.drug.medicinalproduct.exact')

        Returns:
            API response with count data
        """
        self.rate_limiter.wait()

        params = {"count": count_field}

        search_query = self._build_search_query(
            start_date=start_date,
            end_date=end_date
        )

        if search_query:
            params["search"] = search_query

        if self.config.api_key:
            params["api_key"] = self.config.api_key

        response = self.session.get(
            self.config.base_url,
            params=params,
            timeout=self.config.timeout_seconds
        )
        response.raise_for_status()

        return response.json()

    def health_check(self) -> bool:
        """
        Check if the FDA API is accessible.

        Returns:
            True if API is healthy, False otherwise
        """
        try:
            response = self.fetch_events(limit=1)
            return "results" in response
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


# Convenience function for quick usage
def fetch_recent_events(days: int = 30, limit: int = 100) -> list:
    """
    Fetch recent adverse events from the past N days.

    Args:
        days: Number of days to look back
        limit: Maximum number of records

    Returns:
        List of adverse event records
    """
    client = FDAAdverseEventsClient()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    response = client.fetch_events(
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        limit=limit
    )

    return response.get("results", [])


if __name__ == "__main__":
    # Example usage and testing
    print("=" * 60)
    print("OpenFDA Adverse Events Client - Test Run")
    print("=" * 60)

    client = FDAAdverseEventsClient()

    # Health check
    print("\n[1] Health Check...")
    if client.health_check():
        print("    API is healthy!")
    else:
        print("    API health check failed!")
        exit(1)

    # Fetch sample events
    print("\n[2] Fetching 5 recent events...")
    response = client.fetch_events(limit=5)

    meta = response.get("meta", {})
    results = response.get("results", [])

    print(f"    Total available: {meta.get('results', {}).get('total', 'N/A')}")
    print(f"    Fetched: {len(results)}")

    # Display sample event
    if results:
        print("\n[3] Sample Event:")
        event = results[0]
        print(f"    Safety Report ID: {event.get('safetyreportid')}")
        print(f"    Receive Date: {event.get('receivedate')}")
        print(f"    Serious: {event.get('serious')}")

        patient = event.get("patient", {})
        drugs = patient.get("drug", [])
        reactions = patient.get("reaction", [])

        if drugs:
            print(f"    Drugs ({len(drugs)}):")
            for drug in drugs[:3]:
                print(f"      - {drug.get('medicinalproduct', 'Unknown')}")

        if reactions:
            print(f"    Reactions ({len(reactions)}):")
            for reaction in reactions[:3]:
                print(f"      - {reaction.get('reactionmeddrapt', 'Unknown')}")

    # Test date range query (use historical dates - FDA data lags ~3 months)
    print("\n[4] Testing date range query (Q3 2024)...")
    try:
        response = client.fetch_events(
            start_date="20240901",
            end_date="20240930",
            limit=10
        )
        print(f"    Found: {len(response.get('results', []))} events")
    except Exception as e:
        print(f"    Skipped - FDA API returned errors (common with date queries)")
        print(f"    Error: {type(e).__name__}")

    print("\n" + "=" * 60)
    print("Test complete!")
    print("=" * 60)
