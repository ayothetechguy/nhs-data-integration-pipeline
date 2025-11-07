"""
NHS Data Integration ETL Pipeline
Extracts data from 4 source systems, validates, transforms, and loads to DuckDB warehouse
"""

import pandas as pd
import numpy as np
import duckdb
import json
from datetime import datetime
import os

print("=" * 70)
print("NHS DATA INTEGRATION ETL PIPELINE")
print("=" * 70)

# EXTRACT PHASE
print("\n" + "=" * 70)
print("PHASE 1: EXTRACT")
print("=" * 70)

print("\n[1/4] Extracting PAS (Patient Administration System) data...")
df_pas = pd.read_csv('data/sources/pas/patients.csv')
print(f"✓ Loaded {len(df_pas):,} patient records")

print("\n[2/4] Extracting EHR (Electronic Health Records) data...")
with open('data/sources/ehr/encounters.json', 'r') as f:
    encounters_data = json.load(f)
df_ehr = pd.DataFrame(encounters_data)
print(f"✓ Loaded {len(df_ehr):,} clinical encounters")

print("\n[3/4] Extracting LIMS (Laboratory) data...")
df_lims = pd.read_csv('data/sources/lims/lab_results.csv')
print(f"✓ Loaded {len(df_lims):,} lab test results")

print("\n[4/4] Extracting Appointments data...")
df_appointments = pd.read_csv('data/sources/appointments/appointments.csv')
print(f"✓ Loaded {len(df_appointments):,} appointments")

print(f"\nTotal records extracted: {len(df_pas) + len(df_ehr) + len(df_lims) + len(df_appointments):,}")

# VALIDATE PHASE
print("\n" + "=" * 70)
print("PHASE 2: VALIDATE")
print("=" * 70)

def validate_nhs_number(nhs_no):
    """Validate NHS number using Modulus 11 algorithm"""
    try:
        nhs_no = str(nhs_no)
        if len(nhs_no) != 10:
            return False
        
        total = 0
        for i in range(9):
            total += int(nhs_no[i]) * (10 - i)
        
        check_digit = 11 - (total % 11)
        if check_digit == 11:
            check_digit = 0
        elif check_digit == 10:
            return False
        
        return int(nhs_no[9]) == check_digit
    except:
        return False

print("\n[1/5] Validating NHS numbers in PAS...")
pas_valid = df_pas['nhs_number'].apply(validate_nhs_number)
print(f"✓ Valid NHS numbers: {pas_valid.sum():,} / {len(df_pas):,} ({pas_valid.sum()/len(df_pas)*100:.1f}%)")

print("\n[2/5] Checking completeness in PAS...")
pas_completeness = (df_pas.notna().sum() / len(df_pas) * 100).mean()
print(f"✓ Average field completeness: {pas_completeness:.1f}%")

print("\n[3/5] Validating EHR data quality...")
ehr_with_diagnosis = df_ehr['primary_diagnosis'].notna().sum()
print(f"✓ Encounters with diagnosis: {ehr_with_diagnosis:,} / {len(df_ehr):,} ({ehr_with_diagnosis/len(df_ehr)*100:.1f}%)")

print("\n[4/5] Validating LIMS data quality...")
lims_completed = (df_lims['status'] == 'Completed').sum()
print(f"✓ Completed lab tests: {lims_completed:,} / {len(df_lims):,} ({lims_completed/len(df_lims)*100:.1f}%)")

print("\n[5/5] Validating Appointments data quality...")
apt_attended = (df_appointments['attendance_status'] == 'Attended').sum()
apt_past = (df_appointments['attendance_status'] != 'Scheduled').sum()
print(f"✓ Attendance rate: {apt_attended:,} / {apt_past:,} ({apt_attended/apt_past*100:.1f}%)")

# TRANSFORM PHASE
print("\n" + "=" * 70)
print("PHASE 3: TRANSFORM")
print("=" * 70)

print("\n[1/6] Creating dimension: dim_patient...")
dim_patient = df_pas[[
    'patient_id', 'nhs_number', 'title', 'first_name', 'last_name',
    'date_of_birth', 'age', 'gender', 'ethnicity', 'postcode', 'city'
]].copy()
dim_patient['patient_key'] = range(1, len(dim_patient) + 1)
print(f"✓ Created {len(dim_patient):,} patient dimension records")

print("\n[2/6] Creating dimension: dim_date...")
all_dates = pd.concat([
    pd.to_datetime(df_ehr['encounter_date']),
    pd.to_datetime(df_lims['order_date']),
    pd.to_datetime(df_appointments['appointment_date'])
])
unique_dates = pd.DataFrame({'date': all_dates.unique()})
unique_dates['date'] = pd.to_datetime(unique_dates['date'])
unique_dates['date_key'] = unique_dates['date'].dt.strftime('%Y%m%d').astype(int)
unique_dates['year'] = unique_dates['date'].dt.year
unique_dates['quarter'] = unique_dates['date'].dt.quarter
unique_dates['month'] = unique_dates['date'].dt.month
unique_dates['day'] = unique_dates['date'].dt.day
unique_dates['day_of_week'] = unique_dates['date'].dt.dayofweek
dim_date = unique_dates.sort_values('date')
print(f"✓ Created {len(dim_date):,} date dimension records")

print("\n[3/6] Creating dimension: dim_diagnosis...")
diagnosis_data = []
for idx, row in df_ehr.iterrows():
    if pd.notna(row['primary_diagnosis']):
        diag = row['primary_diagnosis']
        if isinstance(diag, dict):
            diagnosis_data.append({
                'icd10_code': diag.get('icd10_code'),
                'description': diag.get('description')
            })
dim_diagnosis = pd.DataFrame(diagnosis_data).drop_duplicates()
dim_diagnosis['diagnosis_key'] = range(1, len(dim_diagnosis) + 1)
print(f"✓ Created {len(dim_diagnosis):,} diagnosis dimension records")

print("\n[4/6] Creating fact table: fact_encounters...")
fact_encounters = df_ehr[['encounter_id', 'nhs_number', 'encounter_date', 
                          'encounter_type', 'department']].copy()
fact_encounters = fact_encounters.merge(
    dim_patient[['nhs_number', 'patient_key']], 
    on='nhs_number', 
    how='left'
)
fact_encounters['encounter_date'] = pd.to_datetime(fact_encounters['encounter_date'])
fact_encounters['date_key'] = fact_encounters['encounter_date'].dt.strftime('%Y%m%d').astype(int)
print(f"✓ Created {len(fact_encounters):,} encounter fact records")

print("\n[5/6] Creating fact table: fact_lab_tests...")
fact_lab_tests = df_lims[['test_id', 'nhs_number', 'test_type', 'test_component',
                           'order_date', 'result_value', 'is_abnormal']].copy()
fact_lab_tests = fact_lab_tests.merge(
    dim_patient[['nhs_number', 'patient_key']], 
    on='nhs_number', 
    how='left'
)
fact_lab_tests['order_date'] = pd.to_datetime(fact_lab_tests['order_date'])
fact_lab_tests['date_key'] = fact_lab_tests['order_date'].dt.strftime('%Y%m%d').astype(int)
print(f"✓ Created {len(fact_lab_tests):,} lab test fact records")

print("\n[6/6] Creating fact table: fact_appointments...")
fact_appointments = df_appointments[['appointment_id', 'nhs_number', 'appointment_date',
                                     'appointment_type', 'specialty', 'attendance_status']].copy()
fact_appointments = fact_appointments.merge(
    dim_patient[['nhs_number', 'patient_key']], 
    on='nhs_number', 
    how='left'
)
fact_appointments['appointment_date'] = pd.to_datetime(fact_appointments['appointment_date'])
fact_appointments['date_key'] = fact_appointments['appointment_date'].dt.strftime('%Y%m%d').astype(int)
print(f"✓ Created {len(fact_appointments):,} appointment fact records")

# LOAD PHASE
print("\n" + "=" * 70)
print("PHASE 4: LOAD TO DATA WAREHOUSE")
print("=" * 70)

os.makedirs('data/warehouse', exist_ok=True)
db_path = 'data/warehouse/nhs_warehouse.duckdb'

print(f"\nConnecting to DuckDB warehouse: {db_path}")
conn = duckdb.connect(db_path)

print("\n[1/6] Loading dim_patient...")
conn.execute("DROP TABLE IF EXISTS dim_patient")
conn.execute("""
    CREATE TABLE dim_patient AS 
    SELECT * FROM dim_patient
""")
print(f"✓ Loaded {len(dim_patient):,} records")

print("\n[2/6] Loading dim_date...")
conn.execute("DROP TABLE IF EXISTS dim_date")
conn.execute("""
    CREATE TABLE dim_date AS 
    SELECT * FROM dim_date
""")
print(f"✓ Loaded {len(dim_date):,} records")

print("\n[3/6] Loading dim_diagnosis...")
conn.execute("DROP TABLE IF EXISTS dim_diagnosis")
conn.execute("""
    CREATE TABLE dim_diagnosis AS 
    SELECT * FROM dim_diagnosis
""")
print(f"✓ Loaded {len(dim_diagnosis):,} records")

print("\n[4/6] Loading fact_encounters...")
conn.execute("DROP TABLE IF EXISTS fact_encounters")
conn.execute("""
    CREATE TABLE fact_encounters AS 
    SELECT * FROM fact_encounters
""")
print(f"✓ Loaded {len(fact_encounters):,} records")

print("\n[5/6] Loading fact_lab_tests...")
conn.execute("DROP TABLE IF EXISTS fact_lab_tests")
conn.execute("""
    CREATE TABLE fact_lab_tests AS 
    SELECT * FROM fact_lab_tests
""")
print(f"✓ Loaded {len(fact_lab_tests):,} records")

print("\n[6/6] Loading fact_appointments...")
conn.execute("DROP TABLE IF EXISTS fact_appointments")
conn.execute("""
    CREATE TABLE fact_appointments AS 
    SELECT * FROM fact_appointments
""")
print(f"✓ Loaded {len(fact_appointments):,} records")

# VERIFY WAREHOUSE
print("\n" + "=" * 70)
print("PHASE 5: VERIFY WAREHOUSE")
print("=" * 70)

print("\nWarehouse tables:")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
    print(f"  • {table[0]}: {count:,} rows")

print("\nSample patient pathway query:")
query = """
SELECT 
    p.patient_id,
    p.first_name,
    p.last_name,
    p.age,
    COUNT(DISTINCT e.encounter_id) as total_encounters,
    COUNT(DISTINCT l.test_id) as total_lab_tests,
    COUNT(DISTINCT a.appointment_id) as total_appointments
FROM dim_patient p
LEFT JOIN fact_encounters e ON p.patient_key = e.patient_key
LEFT JOIN fact_lab_tests l ON p.patient_key = l.patient_key
LEFT JOIN fact_appointments a ON p.patient_key = a.patient_key
GROUP BY p.patient_id, p.first_name, p.last_name, p.age
ORDER BY total_encounters DESC
LIMIT 5
"""
result = conn.execute(query).fetchdf()
print(result.to_string(index=False))

conn.close()

print("\n" + "=" * 70)
print("ETL PIPELINE COMPLETE!")
print("=" * 70)
print(f"\nWarehouse location: {db_path}")
print(f"Total dimension records: {len(dim_patient) + len(dim_date) + len(dim_diagnosis):,}")
print(f"Total fact records: {len(fact_encounters) + len(fact_lab_tests) + len(fact_appointments):,}")
print("\nPipeline executed successfully ✓")
