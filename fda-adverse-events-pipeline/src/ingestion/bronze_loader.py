"""
Bronze Layer Data Loader

Fetches adverse event data from the FDA API and loads it into
the Bronze layer database tables with minimal transformation.
"""

import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional

from .fda_client import FDAAdverseEventsClient, FDAClientConfig
from ..database import (
    init_database,
    session_scope,
    BronzeAdverseEvent,
    IngestionLog
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeLoader:
    """
    Loads raw FDA adverse event data into the Bronze layer.

    Features:
    - Batch tracking with IngestionLog
    - Duplicate detection by safety_report_id
    - Configurable date ranges
    - Progress logging
    """

    def __init__(
        self,
        fda_config: Optional[FDAClientConfig] = None,
        batch_size: int = 100
    ):
        """
        Initialize the Bronze loader.

        Args:
            fda_config: FDA API client configuration
            batch_size: Records per API request
        """
        self.client = FDAAdverseEventsClient(fda_config)
        self.batch_size = batch_size

    def generate_batch_id(self) -> str:
        """Generate a unique batch identifier."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"batch_{timestamp}_{short_uuid}"

    def load(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        drug_name: Optional[str] = None,
        serious_only: bool = False,
        max_records: Optional[int] = None,
        skip_duplicates: bool = True
    ) -> dict:
        """
        Load adverse events into the Bronze layer.

        Args:
            start_date: Start date (YYYY-MM-DD or YYYYMMDD)
            end_date: End date (YYYY-MM-DD or YYYYMMDD)
            drug_name: Filter by drug name
            serious_only: Only serious adverse events
            max_records: Maximum records to load
            skip_duplicates: Skip records already in database

        Returns:
            Dictionary with load statistics
        """
        batch_id = self.generate_batch_id()
        logger.info(f"Starting Bronze load: {batch_id}")

        # Initialize database if needed
        init_database()

        stats = {
            "batch_id": batch_id,
            "records_fetched": 0,
            "records_inserted": 0,
            "records_skipped": 0,
            "records_failed": 0,
            "status": "running"
        }

        # Create ingestion log entry
        with session_scope() as session:
            log_entry = IngestionLog(
                batch_id=batch_id,
                started_at=datetime.utcnow(),
                status="running",
                start_date=start_date,
                end_date=end_date,
                drug_filter=drug_name
            )
            session.add(log_entry)

        try:
            # Stream events from FDA API
            for batch in self.client.stream_events(
                start_date=start_date,
                end_date=end_date,
                drug_name=drug_name,
                serious_only=serious_only,
                batch_size=self.batch_size,
                max_records=max_records
            ):
                batch_stats = self._process_batch(batch, batch_id, skip_duplicates)

                stats["records_fetched"] += batch_stats["fetched"]
                stats["records_inserted"] += batch_stats["inserted"]
                stats["records_skipped"] += batch_stats["skipped"]
                stats["records_failed"] += batch_stats["failed"]

                logger.info(
                    f"Progress: fetched={stats['records_fetched']}, "
                    f"inserted={stats['records_inserted']}, "
                    f"skipped={stats['records_skipped']}"
                )

                # Check max records limit
                if max_records and stats["records_fetched"] >= max_records:
                    break

            stats["status"] = "completed"
            logger.info(f"Bronze load completed: {stats}")

        except Exception as e:
            stats["status"] = "failed"
            stats["error"] = str(e)
            logger.error(f"Bronze load failed: {e}")
            raise

        finally:
            # Update ingestion log
            with session_scope() as session:
                log_entry = session.query(IngestionLog).filter_by(batch_id=batch_id).first()
                if log_entry:
                    log_entry.completed_at = datetime.utcnow()
                    log_entry.status = stats["status"]
                    log_entry.records_fetched = stats["records_fetched"]
                    log_entry.records_inserted = stats["records_inserted"]
                    log_entry.records_failed = stats["records_failed"]
                    if "error" in stats:
                        log_entry.error_message = stats["error"]

        return stats

    def _process_batch(
        self,
        records: list,
        batch_id: str,
        skip_duplicates: bool
    ) -> dict:
        """
        Process a batch of records.

        Args:
            records: List of raw API records
            batch_id: Current batch ID
            skip_duplicates: Whether to skip existing records

        Returns:
            Batch statistics
        """
        stats = {"fetched": len(records), "inserted": 0, "skipped": 0, "failed": 0}

        with session_scope() as session:
            # Get existing safety report IDs for duplicate check
            existing_ids = set()
            if skip_duplicates:
                report_ids = [r.get("safetyreportid") for r in records if r.get("safetyreportid")]
                if report_ids:
                    existing = session.query(BronzeAdverseEvent.safety_report_id).filter(
                        BronzeAdverseEvent.safety_report_id.in_(report_ids)
                    ).all()
                    existing_ids = {row[0] for row in existing}

            # Process each record
            for record in records:
                safety_id = record.get("safetyreportid")

                # Skip duplicates
                if skip_duplicates and safety_id in existing_ids:
                    stats["skipped"] += 1
                    continue

                try:
                    event = BronzeAdverseEvent.from_api_response(record, batch_id)
                    session.add(event)
                    stats["inserted"] += 1
                except Exception as e:
                    logger.warning(f"Failed to process record {safety_id}: {e}")
                    stats["failed"] += 1

        return stats

    def load_recent(self, days: int = 30, max_records: Optional[int] = 1000) -> dict:
        """
        Load recent adverse events from the past N days.

        Args:
            days: Number of days to look back
            max_records: Maximum records to load

        Returns:
            Load statistics
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        return self.load(
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            max_records=max_records
        )


def run_bronze_ingestion(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_records: int = 100,
    drug_name: Optional[str] = None
) -> dict:
    """
    Convenience function to run Bronze layer ingestion.

    Args:
        start_date: Start date filter
        end_date: End date filter
        max_records: Maximum records to ingest
        drug_name: Drug name filter

    Returns:
        Ingestion statistics
    """
    loader = BronzeLoader()
    return loader.load(
        start_date=start_date,
        end_date=end_date,
        drug_name=drug_name,
        max_records=max_records
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load FDA adverse events into Bronze layer")
    parser.add_argument("--start-date", help="Start date (YYYYMMDD or YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYYMMDD or YYYY-MM-DD)")
    parser.add_argument("--max-records", type=int, default=100, help="Maximum records to load")
    parser.add_argument("--drug", help="Filter by drug name")
    parser.add_argument("--serious-only", action="store_true", help="Only serious events")
    parser.add_argument("--recent-days", type=int, help="Load recent N days instead of date range")

    args = parser.parse_args()

    print("=" * 60)
    print("FDA Adverse Events - Bronze Layer Ingestion")
    print("=" * 60)

    loader = BronzeLoader()

    if args.recent_days:
        print(f"\nLoading events from past {args.recent_days} days...")
        stats = loader.load_recent(days=args.recent_days, max_records=args.max_records)
    else:
        print(f"\nLoading events...")
        if args.start_date:
            print(f"  Start date: {args.start_date}")
        if args.end_date:
            print(f"  End date: {args.end_date}")
        if args.drug:
            print(f"  Drug filter: {args.drug}")
        print(f"  Max records: {args.max_records}")

        stats = loader.load(
            start_date=args.start_date,
            end_date=args.end_date,
            drug_name=args.drug,
            serious_only=args.serious_only,
            max_records=args.max_records
        )

    print("\n" + "=" * 60)
    print("Ingestion Results")
    print("=" * 60)
    print(f"  Batch ID: {stats['batch_id']}")
    print(f"  Status: {stats['status']}")
    print(f"  Records fetched: {stats['records_fetched']}")
    print(f"  Records inserted: {stats['records_inserted']}")
    print(f"  Records skipped: {stats['records_skipped']}")
    print(f"  Records failed: {stats['records_failed']}")
    print("=" * 60)
