"""
PAS (Patient Administration System) Data Generator
Generates synthetic patient demographic data in CSV format
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

# Set random seeds for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker('en_GB')
Faker.seed(42)

def calculate_nhs_check_digit(first_nine):
    """Calculate NHS number check digit using Modulus 11 algorithm"""
    total = 0
    for i in range(9):
        total += int(first_nine[i]) * (10 - i)
    
    check_digit = 11 - (total % 11)
    if check_digit == 11:
        check_digit = 0
    elif check_digit == 10:
        return None
    
    return str(check_digit)

def generate_nhs_number():
    """Generate valid NHS number with check digit"""
    while True:
        first_nine = ''.join([str(random.randint(0, 9)) for _ in range(9)])
        check_digit = calculate_nhs_check_digit(first_nine)
        if check_digit is not None:
            return first_nine + check_digit

def generate_pas_data(num_patients=50000):
    """Generate PAS patient demographic data"""
    
    print(f"Generating PAS data for {num_patients:,} patients...")
    
    patients = []
    
    for i in range(num_patients):
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i + 1:,} patients...")
        
        gender = random.choice(['M', 'F', 'Other'])
        
        if gender == 'M':
            first_name = fake.first_name_male()
            title = random.choice(['Mr', 'Dr', 'Rev'])
        elif gender == 'F':
            first_name = fake.first_name_female()
            title = random.choice(['Mrs', 'Miss', 'Ms', 'Dr', 'Rev'])
        else:
            first_name = fake.first_name()
            title = 'Mx'
        
        last_name = fake.last_name()
        
        age_distribution = random.choices(
            population=[
                random.randint(0, 17),
                random.randint(18, 40),
                random.randint(41, 65),
                random.randint(66, 85),
                random.randint(86, 100)
            ],
            weights=[0.15, 0.25, 0.30, 0.25, 0.05]
        )[0]
        
        dob = fake.date_of_birth(minimum_age=age_distribution, maximum_age=age_distribution)
        
        today = datetime.now().date()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        
        nhs_number = generate_nhs_number()
        
        address_line1 = fake.street_address()
        city = random.choice([
            'Edinburgh', 'Glasgow', 'Aberdeen', 'Dundee', 
            'Inverness', 'Stirling', 'Perth', 'Paisley'
        ])
        postcode = fake.postcode()
        
        ethnicity = random.choices(
            population=['White British', 'White Other', 'Asian', 'Black', 'Mixed', 'Other'],
            weights=[0.84, 0.08, 0.04, 0.02, 0.01, 0.01]
        )[0]
        
        gp_practice_code = f"GP{random.randint(10000, 99999)}"
        gp_practice_name = f"{fake.last_name()} Medical Practice"
        
        years_registered = np.random.exponential(scale=10)
        years_registered = min(years_registered, 60)
        registration_date = today - timedelta(days=int(years_registered * 365))
        
        is_active = random.choices([True, False], weights=[0.98, 0.02])[0]
        
        phone = fake.phone_number() if random.random() > 0.05 else None
        email = fake.email() if random.random() > 0.15 else None
        
        nok_name = fake.name() if random.random() > 0.10 else None
        nok_relationship = random.choice(['Spouse', 'Child', 'Parent', 'Sibling', 'Friend']) if nok_name else None
        nok_phone = fake.phone_number() if nok_name and random.random() > 0.05 else None
        
        patients.append({
            'patient_id': f'PAS{i+1:06d}',
            'nhs_number': nhs_number,
            'title': title,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': dob.strftime('%Y-%m-%d'),
            'age': age,
            'gender': gender,
            'ethnicity': ethnicity,
            'address_line1': address_line1,
            'city': city,
            'postcode': postcode,
            'phone': phone,
            'email': email,
            'gp_practice_code': gp_practice_code,
            'gp_practice_name': gp_practice_name,
            'registration_date': registration_date.strftime('%Y-%m-%d'),
            'is_active': is_active,
            'nok_name': nok_name,
            'nok_relationship': nok_relationship,
            'nok_phone': nok_phone,
            'created_date': today.strftime('%Y-%m-%d'),
            'last_updated': today.strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(patients)
    
    print(f"\n✅ Generated {len(df):,} patient records")
    print(f"   Age range: {df['age'].min()}-{df['age'].max()} years")
    print(f"   Gender distribution:")
    print(df['gender'].value_counts())
    print(f"   Active patients: {df['is_active'].sum():,}")
    
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("PAS DATA GENERATOR")
    print("=" * 60)
    
    df_pas = generate_pas_data(num_patients=50000)
    
    output_file = 'data/sources/pas/patients.csv'
    df_pas.to_csv(output_file, index=False)
    
    print(f"\n💾 Data saved to: {output_file}")
    print(f"   File size: {df_pas.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print(f"\n📊 Sample records:")
    print(df_pas.head(3).to_string())
    
    print("\n" + "=" * 60)
    print("PAS DATA GENERATION COMPLETE!")
    print("=" * 60)
