"""
LIMS (Laboratory Information Management System) Data Generator
Generates synthetic laboratory test data in CSV format
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

# Lab test types with reference ranges
LAB_TESTS = {
    'Full Blood Count': {
        'components': {
            'Haemoglobin': {'unit': 'g/L', 'range_min': 115, 'range_max': 165, 'mean': 140, 'std': 15},
            'White Cell Count': {'unit': '10^9/L', 'range_min': 4.0, 'range_max': 11.0, 'mean': 7.5, 'std': 2.0},
            'Platelets': {'unit': '10^9/L', 'range_min': 150, 'range_max': 400, 'mean': 250, 'std': 60}
        }
    },
    'Renal Function': {
        'components': {
            'Creatinine': {'unit': 'umol/L', 'range_min': 60, 'range_max': 120, 'mean': 90, 'std': 20},
            'eGFR': {'unit': 'mL/min/1.73m2', 'range_min': 60, 'range_max': 120, 'mean': 90, 'std': 20},
            'Urea': {'unit': 'mmol/L', 'range_min': 2.5, 'range_max': 7.8, 'mean': 5.0, 'std': 1.5}
        }
    },
    'Liver Function': {
        'components': {
            'ALT': {'unit': 'U/L', 'range_min': 0, 'range_max': 45, 'mean': 25, 'std': 10},
            'Bilirubin': {'unit': 'umol/L', 'range_min': 0, 'range_max': 21, 'mean': 10, 'std': 5},
            'Albumin': {'unit': 'g/L', 'range_min': 35, 'range_max': 50, 'mean': 42, 'std': 5}
        }
    },
    'HbA1c': {
        'components': {
            'HbA1c': {'unit': 'mmol/mol', 'range_min': 20, 'range_max': 42, 'mean': 35, 'std': 8}
        }
    },
    'Lipid Profile': {
        'components': {
            'Total Cholesterol': {'unit': 'mmol/L', 'range_min': 0, 'range_max': 5.0, 'mean': 4.5, 'std': 1.0},
            'LDL Cholesterol': {'unit': 'mmol/L', 'range_min': 0, 'range_max': 3.0, 'mean': 2.5, 'std': 0.8},
            'HDL Cholesterol': {'unit': 'mmol/L', 'range_min': 1.0, 'range_max': 3.0, 'mean': 1.5, 'std': 0.4}
        }
    },
    'CRP': {
        'components': {
            'CRP': {'unit': 'mg/L', 'range_min': 0, 'range_max': 5, 'mean': 2, 'std': 3}
        }
    }
}

def generate_test_result(test_info, is_abnormal=False):
    """Generate a test result value"""
    if is_abnormal:
        # Generate abnormal value (outside reference range)
        if random.random() < 0.5:
            # Below range
            value = np.random.normal(test_info['range_min'] * 0.7, test_info['std'])
        else:
            # Above range
            value = np.random.normal(test_info['range_max'] * 1.3, test_info['std'])
    else:
        # Generate normal value
        value = np.random.normal(test_info['mean'], test_info['std'])
        value = np.clip(value, test_info['range_min'], test_info['range_max'])
    
    return round(value, 1)

def generate_lims_data(num_tests=150000):
    """Generate LIMS laboratory test data"""
    
    print(f"Generating LIMS data for {num_tests:,} lab tests...")
    
    # Load patient data
    df_patients = pd.read_csv('data/sources/pas/patients.csv')
    patient_ids = df_patients['nhs_number'].tolist()
    
    lab_results = []
    
    for i in range(num_tests):
        if (i + 1) % 15000 == 0:
            print(f"  Generated {i + 1:,} lab tests...")
        
        # Select random patient
        nhs_number = random.choice(patient_ids)
        
        # Test order date (within last 2 years)
        days_ago = random.randint(0, 730)
        order_date = datetime.now() - timedelta(days=days_ago)
        
        # Test type
        test_type = random.choice(list(LAB_TESTS.keys()))
        test_info = LAB_TESTS[test_type]
        
        # Result date (1-5 days after order)
        result_days = random.randint(1, 5)
        result_date = order_date + timedelta(days=result_days)
        
        # Ordering clinician
        clinician_id = f'CLIN{random.randint(1000, 9999)}'
        
        # Urgency
        urgency = random.choices(
            ['Routine', 'Urgent', 'Emergency'],
            weights=[0.85, 0.10, 0.05]
        )[0]
        
        # Specimen type
        specimen_type = random.choice(['Blood', 'Urine', 'Serum', 'Plasma'])
        
        # Status
        status = random.choices(
            ['Completed', 'Pending', 'Rejected'],
            weights=[0.95, 0.03, 0.02]
        )[0]
        
        # Generate results for each component
        if status == 'Completed':
            # 15% chance of abnormal results
            is_abnormal = random.random() < 0.15
            
            for component_name, component_info in test_info['components'].items():
                result_value = generate_test_result(component_info, is_abnormal)
                
                # Check if abnormal
                is_abnormal_flag = (
                    result_value < component_info['range_min'] or 
                    result_value > component_info['range_max']
                )
                
                lab_results.append({
                    'test_id': f'LAB{i+1:08d}_{component_name.replace(" ", "_")}',
                    'nhs_number': nhs_number,
                    'test_type': test_type,
                    'test_component': component_name,
                    'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'result_date': result_date.strftime('%Y-%m-%d %H:%M:%S') if status == 'Completed' else None,
                    'result_value': result_value if status == 'Completed' else None,
                    'unit': component_info['unit'],
                    'reference_range_min': component_info['range_min'],
                    'reference_range_max': component_info['range_max'],
                    'is_abnormal': is_abnormal_flag if status == 'Completed' else None,
                    'urgency': urgency,
                    'specimen_type': specimen_type,
                    'status': status,
                    'ordering_clinician': clinician_id,
                    'laboratory': random.choice(['Main Lab', 'Biochemistry', 'Haematology']),
                    'created_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        else:
            # Pending or rejected - no results yet
            for component_name in test_info['components'].keys():
                lab_results.append({
                    'test_id': f'LAB{i+1:08d}_{component_name.replace(" ", "_")}',
                    'nhs_number': nhs_number,
                    'test_type': test_type,
                    'test_component': component_name,
                    'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'result_date': None,
                    'result_value': None,
                    'unit': test_info['components'][component_name]['unit'],
                    'reference_range_min': test_info['components'][component_name]['range_min'],
                    'reference_range_max': test_info['components'][component_name]['range_max'],
                    'is_abnormal': None,
                    'urgency': urgency,
                    'specimen_type': specimen_type,
                    'status': status,
                    'ordering_clinician': clinician_id,
                    'laboratory': random.choice(['Main Lab', 'Biochemistry', 'Haematology']),
                    'created_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
    
    df = pd.DataFrame(lab_results)
    
    print(f"\n✅ Generated {len(df):,} lab test results")
    print(f"   Test types:")
    print(df['test_type'].value_counts())
    print(f"   Abnormal results: {df['is_abnormal'].sum():,} ({df['is_abnormal'].sum()/len(df)*100:.1f}%)")
    
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("LIMS DATA GENERATOR")
    print("=" * 60)
    
    df_lims = generate_lims_data(num_tests=150000)
    
    output_file = 'data/sources/lims/lab_results.csv'
    df_lims.to_csv(output_file, index=False)
    
    print(f"\n💾 Data saved to: {output_file}")
    print(f"   File size: {df_lims.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    print(f"\n📊 Sample lab results:")
    print(df_lims.head(5).to_string())
    
    print("\n" + "=" * 60)
    print("LIMS DATA GENERATION COMPLETE!")
    print("=" * 60)
