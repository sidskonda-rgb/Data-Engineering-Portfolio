"""
Multi-Format Healthcare Data ETL Pipeline (Pandas Alternative)
Processes: FHIR JSON, HL7 v2 messages, OCR'd PDFs, CSV files
Output: 6 bronze layer Parquet tables
Note: Uses pandas instead of Spark for environments without Java/Spark setup
"""
import pandas as pd
import json
import re
import os
from datetime import datetime
from pathlib import Path

def main():
    print("\n" + "="*80)
    print("STARTING MULTI-FORMAT HEALTHCARE DATA ETL PIPELINE (Pandas)")
    print("="*80 + "\n")

    # Base paths
    raw_base = Path("../../data/raw")
    bronze_base = Path("../../data/processed/bronze")

    # Ensure bronze directory exists
    bronze_base.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # 1. PROCESS FHIR JSON DATA
    # ========================================================================
    print("[1/4] Processing FHIR JSON files...")

    try:
        patients_fhir = []
        observations_fhir = []

        # Process each FHIR JSON file
        for fhir_file in (raw_base / "fhir").glob("*.json"):
            with open(fhir_file, 'r', encoding='utf-16') as f:
                bundle = json.load(f)

            # Extract patients and observations from bundle
            for entry in bundle.get("entry", []):
                resource = entry.get("resource", {})
                resource_type = resource.get("resourceType")

                if resource_type == "Patient":
                    # Extract patient demographics
                    name = resource.get("name", [{}])[0]
                    address = resource.get("address", [{}])[0]
                    patients_fhir.append({
                        "patient_id": resource.get("id"),
                        "mrn": None,
                        "family_name": name.get("family"),
                        "given_name": name.get("given", [None])[0],
                        "gender": resource.get("gender"),
                        "birth_date": pd.to_datetime(resource.get("birthDate")) if resource.get("birthDate") else None,
                        "city": address.get("city"),
                        "state": address.get("state"),
                        "source_system": "FHIR_API",
                        "ingested_at": datetime.now()
                    })

                elif resource_type == "Observation":
                    # Extract clinical observations
                    coding = resource.get("code", {}).get("coding", [{}])[0]
                    value_qty = resource.get("valueQuantity", {})
                    observations_fhir.append({
                        "observation_id": resource.get("id"),
                        "patient_ref": resource.get("subject", {}).get("reference"),
                        "loinc_code": coding.get("code"),
                        "observation_name": coding.get("display"),
                        "value": value_qty.get("value"),
                        "unit": value_qty.get("unit"),
                        "observation_time": pd.to_datetime(resource.get("effectiveDateTime")) if resource.get("effectiveDateTime") else None,
                        "source_system": "FHIR_API",
                        "ingested_at": datetime.now()
                    })

        # Convert to DataFrames and write
        df_patients_fhir = pd.DataFrame(patients_fhir)
        df_observations_fhir = pd.DataFrame(observations_fhir)

        (bronze_base / "patients_fhir").mkdir(exist_ok=True)
        (bronze_base / "observations_fhir").mkdir(exist_ok=True)

        df_patients_fhir.to_parquet(bronze_base / "patients_fhir" / "data.parquet", index=False)
        df_observations_fhir.to_parquet(bronze_base / "observations_fhir" / "data.parquet", index=False)

        print(f"   [OK] FHIR: {len(df_patients_fhir)} patients, {len(df_observations_fhir)} observations")

    except Exception as e:
        print(f"   [WARN] FHIR processing failed: {str(e)}")

    # ========================================================================
    # 2. PROCESS HL7 v2 MESSAGES
    # ========================================================================
    print("\n[2/4] Processing HL7 v2 messages...")

    try:
        patients_hl7 = []
        observations_hl7 = []

        # Process each HL7 file
        for hl7_file in (raw_base / "hl7").glob("*.hl7"):
            with open(hl7_file, 'r', encoding='utf-16') as f:
                content = f.read()

            # Split into segments (lines)
            segments = content.strip().split('\n')

            for segment in segments:
                # Remove null bytes and extra spaces
                segment = segment.replace('\x00', '').strip()

                if not segment:
                    continue

                # Split by pipe delimiter
                fields = segment.split('|')

                # Extract Patient Demographics from PID segment
                if segment.startswith('PID'):
                    try:
                        # PID-3: Patient ID (MRN)
                        mrn_field = fields[3] if len(fields) > 3 else ""
                        mrn = mrn_field.split('^')[0] if mrn_field else None

                        # PID-5: Patient Name
                        name_field = fields[5] if len(fields) > 5 else ""
                        name_parts = name_field.split('^')
                        family_name = name_parts[0] if len(name_parts) > 0 else None
                        given_name = name_parts[1] if len(name_parts) > 1 else None

                        # PID-8: Gender
                        gender = fields[8] if len(fields) > 8 else None

                        if mrn:  # Only add if we have an MRN
                            patients_hl7.append({
                                "mrn": mrn,
                                "family_name": family_name,
                                "given_name": given_name,
                                "gender": gender,
                                "source_system": "HL7_v2",
                                "ingested_at": datetime.now()
                            })
                    except Exception as e:
                        print(f"      Warning: Failed to parse PID segment: {e}")

                # Extract Observations from OBX segment
                elif segment.startswith('OBX'):
                    try:
                        # OBX-3: Observation Identifier (LOINC code and name)
                        obs_id_field = fields[3] if len(fields) > 3 else ""
                        obs_parts = obs_id_field.split('^')
                        loinc_code = obs_parts[0] if len(obs_parts) > 0 else None
                        obs_name = obs_parts[1] if len(obs_parts) > 1 else None

                        # OBX-5: Observation Value
                        value = fields[5] if len(fields) > 5 else None

                        # OBX-6: Units
                        unit = fields[6] if len(fields) > 6 else None

                        if loinc_code and value:  # Only add if we have code and value
                            observations_hl7.append({
                                "loinc_code": loinc_code,
                                "observation_name": obs_name,
                                "value": value,
                                "unit": unit,
                                "source_system": "HL7_v2",
                                "ingested_at": datetime.now()
                            })
                    except Exception as e:
                        print(f"      Warning: Failed to parse OBX segment: {e}")

        # Convert to DataFrames and write
        df_patients_hl7 = pd.DataFrame(patients_hl7)
        df_observations_hl7 = pd.DataFrame(observations_hl7)

        (bronze_base / "patients_hl7").mkdir(exist_ok=True)
        (bronze_base / "observations_hl7").mkdir(exist_ok=True)

        df_patients_hl7.to_parquet(bronze_base / "patients_hl7" / "data.parquet", index=False)
        df_observations_hl7.to_parquet(bronze_base / "observations_hl7" / "data.parquet", index=False)

        print(f"   [OK] HL7: {len(df_patients_hl7)} patients, {len(df_observations_hl7)} observations")

    except Exception as e:
        print(f"   [WARN] HL7 processing failed: {str(e)}")

    # ========================================================================
    # 3. PROCESS OCR'D PDF/TXT FILES
    # ========================================================================
    print("\n[3/4] Processing OCR'd PDF text files...")

    try:
        labs_ocr = []

        # Process each text file
        for txt_file in (raw_base / "pdfs").glob("*.txt"):
            with open(txt_file, 'r', encoding='utf-16') as f:
                content = f.read()

            # Remove null bytes
            content = content.replace('\x00', '')

            # Extract MRN
            mrn_match = re.search(r'MRN[:\s]+([0-9]+)', content)
            mrn = mrn_match.group(1) if mrn_match else None

            # Extract lab values (pattern: TestName Value Unit)
            # Look for lines with numeric values and units
            lab_patterns = [
                (r'(Glucose)\s+([0-9.]+)\s+(mg/dL)', 'Glucose'),
                (r'(Creatinine)\s+([0-9.]+)\s+(mg/dL)', 'Creatinine'),
                (r'(Sodium)\s+([0-9.]+)\s+(mmol/L)', 'Sodium'),
                (r'(Potassium)\s+([0-9.]+)\s+(mmol/L)', 'Potassium'),
                (r'(CO2)\s+([0-9.]+)\s+(mmol/L)', 'CO2'),
                (r'(BUN)\s+([0-9.]+)\s+(mg/dL)', 'BUN'),
                (r'(Calcium)\s+([0-9.]+)\s+(mg/dL)', 'Calcium')
            ]

            for pattern, test_name in lab_patterns:
                matches = re.finditer(pattern, content)
                for match in matches:
                    labs_ocr.append({
                        "mrn": mrn,
                        "test_name": test_name,
                        "value": float(match.group(2)),
                        "unit": match.group(3),
                        "source_system": "OCR_PDF",
                        "ingested_at": datetime.now()
                    })

        # Convert to DataFrame and write
        df_labs_ocr = pd.DataFrame(labs_ocr)

        (bronze_base / "labs_ocr").mkdir(exist_ok=True)

        if len(df_labs_ocr) > 0:
            df_labs_ocr.to_parquet(bronze_base / "labs_ocr" / "data.parquet", index=False)

        print(f"   [OK] OCR: {len(df_labs_ocr)} lab results extracted")

    except Exception as e:
        print(f"   [WARN] OCR processing failed: {str(e)}")

    # ========================================================================
    # 4. PROCESS CSV FILES
    # ========================================================================
    print("\n[4/4] Processing CSV files...")

    try:
        # Read deployment readiness CSV
        df_readiness = pd.read_csv(raw_base / "csv" / "deployment_readiness_data.csv", encoding='utf-16')

        # Clean and transform
        df_readiness['medical_clearance'] = df_readiness['medical_clearance'].map({
            'YES': True,
            'NO': False
        })

        df_readiness['deployable'] = df_readiness['deployable'].map({
            'TRUE': True,
            'FALSE': False
        })

        df_readiness['dental_class'] = pd.to_numeric(df_readiness['dental_class'], errors='coerce')
        df_readiness['bmi'] = pd.to_numeric(df_readiness['bmi'], errors='coerce')
        df_readiness['last_pha_date'] = pd.to_datetime(df_readiness['last_pha_date'], errors='coerce')

        # Add metadata
        df_readiness['source_system'] = 'MEDPROS_CSV'
        df_readiness['ingested_at'] = datetime.now()

        # Write to parquet
        (bronze_base / "readiness_csv").mkdir(exist_ok=True)
        df_readiness.to_parquet(bronze_base / "readiness_csv" / "data.parquet", index=False)

        print(f"   [OK] CSV: {len(df_readiness)} soldier readiness records")

    except Exception as e:
        print(f"   [WARN] CSV processing failed: {str(e)}")

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("ETL PIPELINE COMPLETE - Bronze Layer Created")
    print("="*80)
    print(f"\nOutput location: {bronze_base}/")
    print("\nTables created:")
    print("   1. patients_fhir/")
    print("   2. observations_fhir/")
    print("   3. patients_hl7/")
    print("   4. observations_hl7/")
    print("   5. labs_ocr/")
    print("   6. readiness_csv/")
    print("\n[OK] Ready for dbt transformation (Silver/Gold layers)\n")

if __name__ == "__main__":
    main()
