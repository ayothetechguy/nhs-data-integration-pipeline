"""
EHR (Electronic Health Records) Data Generator
Generates synthetic clinical encounter data in JSON format
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

# Common ICD-10 diagnosis codes
ICD10_CODES = {
    'I10': 'Essential hypertension',
    'E11.9': 'Type 2 diabetes without complications',
    'J44.0': 'Chronic obstructive pulmonary disease',
    'I25.1': 'Coronary heart disease',
    'M17.0': 'Bilateral primary osteoarthritis of knee',
    'F32.9': 'Major depressive disorder',
    'J18.1': 'Lobar pneumonia',
    'I21.0': 'Acute myocardial infarction',
    'N18.3': 'Chronic kidney disease stage 3',
    'E78.5': 'Hyperlipidaemia',
    'K21.9': 'Gastro-oesophageal reflux disease',
    'M54.5': 'Low back pain',
    'J45.9': 'Asthma',
    'N39.0': 'Urinary tract infection',
    'I50.0': 'Congestive heart failure'
}

# Common medications
MEDICATIONS = {
    'I10': ['Amlodipine', 'Ramipril', 'Losartan'],
    'E11.9': ['Metformin', 'Gliclazide', 'Insulin'],
    'J44.0': ['Salbutamol', 'Tiotropium', 'Prednisolone'],
    'I25.1': ['Aspirin', 'Atorvastatin', 'Bisoprolol'],
    'F32.9': ['Sertraline', 'Citalopram', 'Fluoxetine'],
    'J18.1': ['Amoxicillin', 'Clarithromycin', 'Doxycycline'],
    'J45.9': ['Salbutamol', 'Beclometasone', 'Montelukast']
}

def generate_ehr_data(num_encounters=100000):
    """Generate EHR clinical encounter data"""
    
    print(f"Generating EHR data for {num_encounters:,} encounters...")
    
    # Load patient data
    df_patients = pd.read_csv('data/sources/pas/patients.csv')
    patient_ids = df_patients['nhs_number'].tolist()
    
    encounters = []
    
    for i in range(num_encounters):
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1:,} encounters...")
        
        # Select random patient
        nhs_number = random.choice(patient_ids)
        
        # Encounter date (within last 2 years)
        days_ago = random.randint(0, 730)
        encounter_date = datetime.now() - timedelta(days=days_ago)
        
        # Encounter type
        encounter_type = random.choices(
            ['Emergency', 'Outpatient', 'Inpatient', 'GP Visit'],
            weights=[0.15, 0.30, 0.25, 0.30]
        )[0]
        
        # Department
        if encounter_type == 'Emergency':
            department = 'Emergency Department'
        elif encounter_type == 'Outpatient':
            department = random.choice(['Cardiology', 'Respiratory', 'Orthopaedics', 'General Medicine'])
        elif encounter_type == 'Inpatient':
            department = random.choice(['Medical Ward', 'Surgical Ward', 'ICU', 'Cardiology Ward'])
        else:
            department = 'General Practice'
        
        # Primary diagnosis
        icd10_code = random.choice(list(ICD10_CODES.keys()))
        diagnosis_description = ICD10_CODES[icd10_code]
        
        # Secondary diagnoses (20% chance)
        secondary_diagnoses = []
        if random.random() < 0.20:
            num_secondary = random.randint(1, 2)
            secondary_codes = random.sample([k for k in ICD10_CODES.keys() if k != icd10_code], num_secondary)
            secondary_diagnoses = [{'code': code, 'description': ICD10_CODES[code]} for code in secondary_codes]
        
        # Medications
        medications_prescribed = []
        if icd10_code in MEDICATIONS:
            num_meds = random.randint(1, 3)
            meds = random.sample(MEDICATIONS[icd10_code], min(num_meds, len(MEDICATIONS[icd10_code])))
            for med in meds:
                medications_prescribed.append({
                    'medication': med,
                    'dose': f"{random.choice([5, 10, 20, 40, 80])}mg",
                    'frequency': random.choice(['Once daily', 'Twice daily', 'Three times daily', 'As needed']),
                    'duration': f"{random.choice([7, 14, 28, 84])} days"
                })
        
        # Clinical notes
        note_templates = [
            f"Patient presented with {diagnosis_description.lower()}. ",
            f"Examination revealed signs consistent with {diagnosis_description.lower()}. ",
            f"Patient reports worsening symptoms of {diagnosis_description.lower()}. ",
            f"Follow-up visit for {diagnosis_description.lower()}. "
        ]
        clinical_note = random.choice(note_templates)
        clinical_note += random.choice([
            "Treatment plan discussed with patient. ",
            "Medications prescribed as above. ",
            "Patient advised to follow up in clinic. ",
            "Referred for further investigation. "
        ])
        
        # Vitals
        vitals = {
            'blood_pressure_systolic': random.randint(100, 180),
            'blood_pressure_diastolic': random.randint(60, 110),
            'heart_rate': random.randint(55, 120),
            'temperature': round(random.uniform(36.0, 39.0), 1),
            'oxygen_saturation': random.randint(88, 100)
        }
        
        # Lab tests ordered (30% of encounters)
        lab_tests = []
        if random.random() < 0.30:
            possible_tests = ['Full Blood Count', 'Renal Function', 'Liver Function', 'HbA1c', 'Lipid Profile', 'CRP']
            num_tests = random.randint(1, 3)
            lab_tests = random.sample(possible_tests, num_tests)
        
        # Discharge disposition (for inpatient/emergency)
        if encounter_type in ['Inpatient', 'Emergency']:
            disposition = random.choices(
                ['Discharged Home', 'Admitted', 'Transferred', 'Left Before Completion'],
                weights=[0.70, 0.20, 0.05, 0.05]
            )[0]
            
            if disposition == 'Admitted' or encounter_type == 'Inpatient':
                length_of_stay = random.randint(1, 14)
                discharge_date = encounter_date + timedelta(days=length_of_stay)
            else:
                length_of_stay = None
                discharge_date = encounter_date
        else:
            disposition = 'Completed'
            length_of_stay = None
            discharge_date = encounter_date
        
        encounter = {
            'encounter_id': f'ENC{i+1:08d}',
            'nhs_number': nhs_number,
            'encounter_date': encounter_date.strftime('%Y-%m-%d %H:%M:%S'),
            'encounter_type': encounter_type,
            'department': department,
            'primary_diagnosis': {
                'icd10_code': icd10_code,
                'description': diagnosis_description
            },
            'secondary_diagnoses': secondary_diagnoses,
            'medications': medications_prescribed,
            'clinical_note': clinical_note,
            'vitals': vitals,
            'lab_tests_ordered': lab_tests,
            'disposition': disposition,
            'length_of_stay_days': length_of_stay,
            'discharge_date': discharge_date.strftime('%Y-%m-%d %H:%M:%S') if discharge_date else None,
            'clinician_id': f'CLIN{random.randint(1000, 9999)}',
            'created_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        encounters.append(encounter)
    
    print(f"\n✅ Generated {len(encounters):,} clinical encounters")
    print(f"   Encounter types:")
    types = pd.DataFrame(encounters)['encounter_type'].value_counts()
    print(types)
    
    return encounters

if __name__ == "__main__":
    print("=" * 60)
    print("EHR DATA GENERATOR")
    print("=" * 60)
    
    encounters = generate_ehr_data(num_encounters=100000)
    
    # Save as JSON (one file per encounter for realism)
    # In real systems, each encounter might be a separate document
    output_file = 'data/sources/ehr/encounters.json'
    
    with open(output_file, 'w') as f:
        json.dump(encounters, f, indent=2)
    
    print(f"\n💾 Data saved to: {output_file}")
    
    # Show sample
    print(f"\n📊 Sample encounter:")
    print(json.dumps(encounters[0], indent=2))
    
    print("\n" + "=" * 60)
    print("EHR DATA GENERATION COMPLETE!")
    print("=" * 60)
