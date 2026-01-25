# Ingestion module - API clients and data extraction
from .fda_client import FDAAdverseEventsClient, FDAClientConfig, fetch_recent_events
from .models import AdverseEvent, Patient, Drug, Reaction
from .bronze_loader import BronzeLoader, run_bronze_ingestion

__all__ = [
    "FDAAdverseEventsClient",
    "FDAClientConfig",
    "fetch_recent_events",
    "AdverseEvent",
    "Patient",
    "Drug",
    "Reaction",
    "BronzeLoader",
    "run_bronze_ingestion"
]
