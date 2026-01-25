"""
SQLAlchemy models for the Bronze layer.

Bronze layer stores raw data with minimal transformation,
preserving the original structure from the FDA API.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Boolean, Float, DateTime,
    Text, ForeignKey, Index, JSON
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class IngestionLog(Base):
    """
    Track ingestion batches for monitoring and debugging.

    Each batch represents a single ingestion run, recording
    metadata about what was fetched and when.
    """
    __tablename__ = "ingestion_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(50), unique=True, nullable=False, index=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="running")  # running, completed, failed

    # Query parameters used
    start_date = Column(String(10), nullable=True)
    end_date = Column(String(10), nullable=True)
    drug_filter = Column(String(255), nullable=True)

    # Results
    records_fetched = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)

    def __repr__(self):
        return f"<IngestionLog(batch_id={self.batch_id}, status={self.status})>"


class BronzeAdverseEvent(Base):
    """
    Raw adverse event reports from the FDA API.

    This table stores the core event data with minimal transformation.
    Nested data (drugs, reactions) are stored in separate tables.
    """
    __tablename__ = "bronze_adverse_events"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # FDA identifiers
    safety_report_id = Column(String(50), nullable=False, index=True)
    safety_report_version = Column(String(10), nullable=True)

    # Dates (stored as strings, matching API format YYYYMMDD)
    receive_date = Column(String(8), nullable=True, index=True)
    receipt_date = Column(String(8), nullable=True)
    transmission_date = Column(String(8), nullable=True)

    # Location
    occur_country = Column(String(10), nullable=True)

    # Seriousness flags
    serious = Column(Boolean, default=False)
    seriousness_death = Column(Boolean, default=False)
    seriousness_life_threatening = Column(Boolean, default=False)
    seriousness_hospitalization = Column(Boolean, default=False)
    seriousness_disability = Column(Boolean, default=False)
    seriousness_congenital_anomaly = Column(Boolean, default=False)
    seriousness_other = Column(Boolean, default=False)

    # Report metadata
    company_numb = Column(String(50), nullable=True)
    duplicate = Column(Boolean, default=False)
    report_type = Column(String(5), nullable=True)

    # Reporter info
    reporter_country = Column(String(10), nullable=True)
    reporter_qualification = Column(String(5), nullable=True)

    # Patient demographics
    patient_sex = Column(String(5), nullable=True)
    patient_age = Column(Float, nullable=True)
    patient_age_unit = Column(String(10), nullable=True)
    patient_weight = Column(Float, nullable=True)

    # Raw JSON for complete data preservation
    raw_json = Column(JSON, nullable=True)

    # Pipeline metadata
    batch_id = Column(String(50), nullable=True, index=True)
    ingested_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    drugs = relationship("BronzeDrug", back_populates="adverse_event", cascade="all, delete-orphan")
    reactions = relationship("BronzeReaction", back_populates="adverse_event", cascade="all, delete-orphan")

    # Indexes for common queries
    __table_args__ = (
        Index("ix_bronze_ae_receive_date_serious", "receive_date", "serious"),
        Index("ix_bronze_ae_batch_receive", "batch_id", "receive_date"),
    )

    def __repr__(self):
        return f"<BronzeAdverseEvent(safety_report_id={self.safety_report_id})>"

    @classmethod
    def from_api_response(cls, data: dict, batch_id: Optional[str] = None) -> "BronzeAdverseEvent":
        """
        Create instance from raw API response.

        Args:
            data: Raw API response dictionary
            batch_id: Batch identifier for tracking

        Returns:
            BronzeAdverseEvent instance (not yet added to session)
        """
        patient = data.get("patient", {})
        primary_source = data.get("primarysource", {})

        event = cls(
            safety_report_id=data.get("safetyreportid", ""),
            safety_report_version=data.get("safetyreportversion"),
            receive_date=data.get("receivedate"),
            receipt_date=data.get("receiptdate"),
            transmission_date=data.get("transmissiondate"),
            occur_country=data.get("occurcountry"),
            serious=data.get("serious") == "1",
            seriousness_death=data.get("seriousnessdeath") == "1",
            seriousness_life_threatening=data.get("seriousnesslifethreatening") == "1",
            seriousness_hospitalization=data.get("seriousnesshospitalization") == "1",
            seriousness_disability=data.get("seriousnessdisabling") == "1",
            seriousness_congenital_anomaly=data.get("seriousnesscongenitalanomali") == "1",
            seriousness_other=data.get("seriousnessother") == "1",
            company_numb=data.get("companynumb"),
            duplicate=data.get("duplicate") == "1",
            report_type=data.get("reporttype"),
            reporter_country=primary_source.get("reportercountry"),
            reporter_qualification=primary_source.get("qualification"),
            patient_sex=patient.get("patientsex"),
            patient_age=float(patient["patientonsetage"]) if patient.get("patientonsetage") else None,
            patient_age_unit=patient.get("patientonsetageunit"),
            patient_weight=float(patient["patientweight"]) if patient.get("patientweight") else None,
            raw_json=data,
            batch_id=batch_id,
            ingested_at=datetime.utcnow()
        )

        # Create drug records
        for drug_data in patient.get("drug", []):
            drug = BronzeDrug.from_api_response(drug_data)
            event.drugs.append(drug)

        # Create reaction records
        for reaction_data in patient.get("reaction", []):
            reaction = BronzeReaction.from_api_response(reaction_data)
            event.reactions.append(reaction)

        return event


class BronzeDrug(Base):
    """
    Drugs associated with adverse events.

    Each adverse event can have multiple drugs with different roles
    (suspect, concomitant, interacting).
    """
    __tablename__ = "bronze_drugs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    adverse_event_id = Column(Integer, ForeignKey("bronze_adverse_events.id"), nullable=False)

    # Drug identification
    medicinal_product = Column(String(500), nullable=True, index=True)
    drug_characterization = Column(String(5), nullable=True)  # 1=Suspect, 2=Concomitant, 3=Interacting

    # Drug details
    drug_indication = Column(String(500), nullable=True)
    drug_administration_route = Column(String(50), nullable=True)
    drug_dosage_text = Column(Text, nullable=True)
    drug_dosage_form = Column(String(100), nullable=True)
    drug_authorization_numb = Column(String(50), nullable=True)

    # Active substance
    active_substance_name = Column(String(500), nullable=True, index=True)

    # OpenFDA enrichment (if available)
    openfda_brand_name = Column(String(500), nullable=True)
    openfda_generic_name = Column(String(500), nullable=True)
    openfda_manufacturer = Column(String(500), nullable=True)
    openfda_product_ndc = Column(String(100), nullable=True)

    # Relationship
    adverse_event = relationship("BronzeAdverseEvent", back_populates="drugs")

    __table_args__ = (
        Index("ix_bronze_drugs_product_char", "medicinal_product", "drug_characterization"),
    )

    def __repr__(self):
        return f"<BronzeDrug(medicinal_product={self.medicinal_product})>"

    @classmethod
    def from_api_response(cls, data: dict) -> "BronzeDrug":
        """Create instance from API drug object."""
        openfda = data.get("openfda", {})
        active_substance = data.get("activesubstance", {})

        return cls(
            medicinal_product=data.get("medicinalproduct"),
            drug_characterization=data.get("drugcharacterization"),
            drug_indication=data.get("drugindication"),
            drug_administration_route=data.get("drugadministrationroute"),
            drug_dosage_text=data.get("drugdosagetext"),
            drug_dosage_form=data.get("drugdosageform"),
            drug_authorization_numb=data.get("drugauthorizationnumb"),
            active_substance_name=active_substance.get("activesubstancename"),
            openfda_brand_name=openfda.get("brand_name", [None])[0] if openfda.get("brand_name") else None,
            openfda_generic_name=openfda.get("generic_name", [None])[0] if openfda.get("generic_name") else None,
            openfda_manufacturer=openfda.get("manufacturer_name", [None])[0] if openfda.get("manufacturer_name") else None,
            openfda_product_ndc=openfda.get("product_ndc", [None])[0] if openfda.get("product_ndc") else None
        )


class BronzeReaction(Base):
    """
    Adverse reactions reported in events.

    Uses MedDRA terminology for standardized reaction names.
    """
    __tablename__ = "bronze_reactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    adverse_event_id = Column(Integer, ForeignKey("bronze_adverse_events.id"), nullable=False)

    # Reaction details (MedDRA coded)
    reaction_meddra_pt = Column(String(500), nullable=True, index=True)  # Preferred Term
    reaction_meddra_version = Column(String(10), nullable=True)
    reaction_outcome = Column(String(5), nullable=True)  # 1-6 scale

    # Relationship
    adverse_event = relationship("BronzeAdverseEvent", back_populates="reactions")

    def __repr__(self):
        return f"<BronzeReaction(reaction_meddra_pt={self.reaction_meddra_pt})>"

    @classmethod
    def from_api_response(cls, data: dict) -> "BronzeReaction":
        """Create instance from API reaction object."""
        return cls(
            reaction_meddra_pt=data.get("reactionmeddrapt"),
            reaction_meddra_version=data.get("reactionmeddraversionpt"),
            reaction_outcome=data.get("reactionoutcome")
        )
