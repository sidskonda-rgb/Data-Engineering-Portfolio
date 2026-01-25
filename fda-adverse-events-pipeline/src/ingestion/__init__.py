# Ingestion module - API clients and data extraction
from .fda_client import FDAAdverseEventsClient, FDAClientConfig, fetch_recent_events
from .models import AdverseEvent, Patient, Drug, Reaction

__all__ = [
    "FDAAdverseEventsClient",
    "FDAClientConfig",
    "fetch_recent_events",
    "AdverseEvent",
    "Patient",
    "Drug",
    "Reaction"
]
