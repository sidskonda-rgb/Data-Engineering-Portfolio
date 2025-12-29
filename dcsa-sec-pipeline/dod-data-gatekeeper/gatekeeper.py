import pandas as pd
import os
import shutil

# 1. Define expectations (Simulating a Glue Data Catalog schema)
EXPECTED_COLUMNS = ['id', 'name', 'amount', 'transaction_date']

def validate_file(file_path):
    df = pd.read_csv(file_path)
    errors = []

    # Check for Schema Drift
    if list(df.columns) != EXPECTED_COLUMNS:
        errors.append(f"Schema Mismatch: Expected {EXPECTED_COLUMNS}")

    # Check for Nulls in critical fields
    if df['name'].isnull().any():
        errors.append("Critical Error: Null values found in 'name' column")

    # Check for Data Type integrity
    try:
        pd.to_numeric(df['amount'])
    except:
        errors.append("Type Error: 'amount' column contains non-numeric data")

    return errors

# 2. Process the "Landing Zone"
for filename in os.listdir('landing_zone'):
    path = os.path.join('landing_zone', filename)
    validation_errors = validate_file(path)

    if not validation_errors:
        print(f"✅ {filename} passed. Moving to Production.")
        shutil.move(path, f"production_zone/{filename}")
    else:
        print(f"❌ {filename} FAILED. Errors: {validation_errors}")
        # In a real DoD environment, you'd log this to AWS CloudWatch