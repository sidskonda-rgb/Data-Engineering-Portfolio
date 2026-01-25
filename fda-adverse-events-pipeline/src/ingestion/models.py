"""
Data models for FDA Adverse Events.

These models represent the structure of adverse event reports
from the OpenFDA API, providing type safety and validation.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class PatientSex(Enum):
    """Patient sex codes from FDA."""
    UNKNOWN = "0"
    MALE = "1"
    FEMALE = "2"


class DrugCharacterization(Enum):
    """Drug role in the adverse event."""
    SUSPECT = "1"      # Suspect drug
    CONCOMITANT = "2"  # Concomitant drug
    INTERACTING = "3"  # Interacting drug


class SeriousnessType(Enum):
    """Types of serious outcomes."""
    DEATH = "seriousnessdeath"
    LIFE_THREATENING = "seriousnesslifethreatening"
    HOSPITALIZATION = "seriousnesshospitalization"
    DISABILITY = "seriousnessdisabling"
    CONGENITAL_ANOMALY = "seriousnesscongenitalanomali"
    OTHER = "seriousnessother"


@dataclass
class Drug:
    """A drug involved in an adverse event."""
    medicinal_product: str
    drug_characterization: Optional[str] = None
    drug_indication: Optional[str] = None
    drug_administration_route: Optional[str] = None
    drug_dosage_text: Optional[str] = None
    drug_authorization_numb: Optional[str] = None
    active_substance_name: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: dict) -> "Drug":
        """Create Drug from API response."""
        return cls(
            medicinal_product=data.get("medicinalproduct", "UNKNOWN"),
            drug_characterization=data.get("drugcharacterization"),
            drug_indication=data.get("drugindication"),
            drug_administration_route=data.get("drugadministrationroute"),
            drug_dosage_text=data.get("drugdosagetext"),
            drug_authorization_numb=data.get("drugauthorizationnumb"),
            active_substance_name=data.get("activesubstance", {}).get("activesubstancename")
        )


@dataclass
class Reaction:
    """An adverse reaction reported."""
    reaction_meddra_pt: str  # MedDRA Preferred Term
    reaction_outcome: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: dict) -> "Reaction":
        """Create Reaction from API response."""
        return cls(
            reaction_meddra_pt=data.get("reactionmeddrapt", "UNKNOWN"),
            reaction_outcome=data.get("reactionoutcome")
        )


@dataclass
class Patient:
    """Patient information from an adverse event report."""
    patient_sex: Optional[str] = None
    patient_age: Optional[float] = None
    patient_age_unit: Optional[str] = None
    patient_weight: Optional[float] = None
    drugs: list[Drug] = field(default_factory=list)
    reactions: list[Reaction] = field(default_factory=list)

    @classmethod
    def from_api_response(cls, data: dict) -> "Patient":
        """Create Patient from API response."""
        drugs = [
            Drug.from_api_response(d)
            for d in data.get("drug", [])
        ]
        reactions = [
            Reaction.from_api_response(r)
            for r in data.get("reaction", [])
        ]

        return cls(
            patient_sex=data.get("patientsex"),
            patient_age=float(data["patientonsetage"]) if data.get("patientonsetage") else None,
            patient_age_unit=data.get("patientonsetageunit"),
            patient_weight=float(data["patientweight"]) if data.get("patientweight") else None,
            drugs=drugs,
            reactions=reactions
        )


@dataclass
class AdverseEvent:
    """
    A complete adverse event report from the FDA.

    This represents a single report submitted to the FDA's
    Adverse Event Reporting System (FAERS).
    """
    safety_report_id: str
    receive_date: str
    receipt_date: Optional[str] = None
    transmission_date: Optional[str] = None
    occur_country: Optional[str] = None
    serious: bool = False
    seriousness_death: bool = False
    seriousness_life_threatening: bool = False
    seriousness_hospitalization: bool = False
    seriousness_disability: bool = False
    seriousness_congenital_anomaly: bool = False
    seriousness_other: bool = False
    company_numb: Optional[str] = None
    duplicate: bool = False
    reporter_country: Optional[str] = None
    reporter_qualification: Optional[str] = None
    patient: Optional[Patient] = None

    # Metadata for pipeline tracking
    ingested_at: Optional[datetime] = None
    batch_id: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: dict, batch_id: Optional[str] = None) -> "AdverseEvent":
        """
        Create AdverseEvent from API response.

        Args:
            data: Raw API response dictionary
            batch_id: Optional batch identifier for tracking

        Returns:
            AdverseEvent instance
        """
        patient_data = data.get("patient", {})
        primary_source = data.get("primarysource", {})

        return cls(
            safety_report_id=data.get("safetyreportid", ""),
            receive_date=data.get("receivedate", ""),
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
            reporter_country=primary_source.get("reportercountry"),
            reporter_qualification=primary_source.get("qualification"),
            patient=Patient.from_api_response(patient_data) if patient_data else None,
            ingested_at=datetime.utcnow(),
            batch_id=batch_id
        )

    def to_flat_dict(self) -> dict:
        """
        Convert to flat dictionary for database insertion.

        Flattens nested structures for easier storage in Bronze layer.
        """
        return {
            "safety_report_id": self.safety_report_id,
            "receive_date": self.receive_date,
            "receipt_date": self.receipt_date,
            "transmission_date": self.transmission_date,
            "occur_country": self.occur_country,
            "serious": self.serious,
            "seriousness_death": self.seriousness_death,
            "seriousness_life_threatening": self.seriousness_life_threatening,
            "seriousness_hospitalization": self.seriousness_hospitalization,
            "seriousness_disability": self.seriousness_disability,
            "seriousness_congenital_anomaly": self.seriousness_congenital_anomaly,
            "seriousness_other": self.seriousness_other,
            "company_numb": self.company_numb,
            "duplicate": self.duplicate,
            "reporter_country": self.reporter_country,
            "reporter_qualification": self.reporter_qualification,
            "patient_sex": self.patient.patient_sex if self.patient else None,
            "patient_age": self.patient.patient_age if self.patient else None,
            "patient_age_unit": self.patient.patient_age_unit if self.patient else None,
            "patient_weight": self.patient.patient_weight if self.patient else None,
            "drug_count": len(self.patient.drugs) if self.patient else 0,
            "reaction_count": len(self.patient.reactions) if self.patient else 0,
            "ingested_at": self.ingested_at,
            "batch_id": self.batch_id
        }

    @property
    def primary_drug(self) -> Optional[str]:
        """Get the primary suspect drug name."""
        if self.patient and self.patient.drugs:
            for drug in self.patient.drugs:
                if drug.drug_characterization == "1":  # Suspect drug
                    return drug.medicinal_product
            return self.patient.drugs[0].medicinal_product
        return None

    @property
    def primary_reaction(self) -> Optional[str]:
        """Get the primary adverse reaction."""
        if self.patient and self.patient.reactions:
            return self.patient.reactions[0].reaction_meddra_pt
        return None

    @property
    def all_drugs(self) -> list[str]:
        """Get list of all drug names."""
        if self.patient:
            return [d.medicinal_product for d in self.patient.drugs]
        return []

    @property
    def all_reactions(self) -> list[str]:
        """Get list of all reaction names."""
        if self.patient:
            return [r.reaction_meddra_pt for r in self.patient.reactions]
        return []
