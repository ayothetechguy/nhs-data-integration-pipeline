"""
Appointments System Data Generator
Generates synthetic appointment scheduling data in CSV format
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

def generate_appointments_data(num_appointments=120000):
    """Generate appointment scheduling data"""
    
    print(f"Generating Appointments data for {num_appointments:,} appointments...")
    
    # Load patient data
    df_patients = pd.read_csv('data/sources/pas/patients.csv')
    patient_ids = df_patients['nhs_number'].tolist()
    
    appointments = []
    
    for i in range(num_appointments):
        if (i + 1) % 12000 == 0:
            print(f"  Generated {i + 1:,} appointments...")
        
        # Select random patient
        nhs_number = random.choice(patient_ids)
        
        # Appointment date (within last 18 months and next 3 months)
        days_offset = random.randint(-545, 90)  # -18 months to +3 months
        appointment_date = datetime.now() + timedelta(days=days_offset)
        
        # Booking date (1-90 days before appointment for future, same day for past)
        if days_offset > 0:
            booking_days_before = random.randint(1, 90)
            booking_date = appointment_date - timedelta(days=booking_days_before)
        else:
            booking_days_before = random.randint(1, 60)
            booking_date = appointment_date - timedelta(days=booking_days_before)
        
        # Appointment type
        appointment_type = random.choices(
            ['GP Consultation', 'Specialist Outpatient', 'Follow-up', 'Diagnostic', 'Treatment', 'Mental Health'],
            weights=[0.35, 0.25, 0.20, 0.10, 0.07, 0.03]
        )[0]
        
        # Department/Specialty
        if appointment_type == 'GP Consultation':
            department = 'General Practice'
            specialty = 'General Medicine'
        elif appointment_type == 'Specialist Outpatient':
            specialty = random.choice(['Cardiology', 'Respiratory', 'Orthopaedics', 'Gastroenterology', 'Neurology', 'Dermatology'])
            department = f'{specialty} Outpatients'
        elif appointment_type == 'Mental Health':
            specialty = 'Psychiatry'
            department = 'Mental Health Services'
        else:
            specialty = random.choice(['General Medicine', 'Surgery', 'Radiology'])
            department = f'{specialty} Department'
        
        # Clinician
        clinician_id = f'CLIN{random.randint(1000, 9999)}'
        clinician_name = f'Dr. {random.choice(["Smith", "Jones", "Brown", "Wilson", "Taylor", "Davies"])}'
        
        # Scheduled time (working hours 9am-5pm)
        hour = random.randint(9, 16)
        minute = random.choice([0, 15, 30, 45])
        appointment_datetime = appointment_date.replace(hour=hour, minute=minute, second=0)
        
        # Duration (in minutes)
        duration_minutes = random.choices(
            [15, 20, 30, 45, 60],
            weights=[0.30, 0.25, 0.30, 0.10, 0.05]
        )[0]
        
        # For past appointments, determine attendance status
        if days_offset < 0:  # Past appointment
            attendance_status = random.choices(
                ['Attended', 'DNA (Did Not Attend)', 'Cancelled by Patient', 'Cancelled by Hospital', 'Rescheduled'],
                weights=[0.75, 0.08, 0.07, 0.05, 0.05]
            )[0]
            
            if attendance_status == 'Attended':
                actual_arrival_time = appointment_datetime + timedelta(minutes=random.randint(-10, 30))
                actual_start_time = appointment_datetime + timedelta(minutes=random.randint(0, 45))
                actual_end_time = actual_start_time + timedelta(minutes=duration_minutes + random.randint(-5, 15))
                wait_time_minutes = int((actual_start_time - actual_arrival_time).total_seconds() / 60)
                cancellation_reason = None
            elif attendance_status == 'DNA (Did Not Attend)':
                actual_arrival_time = None
                actual_start_time = None
                actual_end_time = None
                wait_time_minutes = None
                cancellation_reason = None
            else:  # Cancelled
                actual_arrival_time = None
                actual_start_time = None
                actual_end_time = None
                wait_time_minutes = None
                if 'Patient' in attendance_status:
                    cancellation_reason = random.choice([
                        'Personal reasons', 'Feeling better', 'Transport issues', 
                        'Work commitments', 'Illness', 'Other appointment'
                    ])
                else:
                    cancellation_reason = random.choice([
                        'Clinician unavailable', 'Emergency case', 'Clinic overrunning',
                        'Equipment failure', 'Staff shortage'
                    ])
        else:  # Future appointment
            attendance_status = 'Scheduled'
            actual_arrival_time = None
            actual_start_time = None
            actual_end_time = None
            wait_time_minutes = None
            cancellation_reason = None
        
        # Priority
        priority = random.choices(
            ['Routine', 'Soon', 'Urgent', 'Emergency'],
            weights=[0.70, 0.20, 0.08, 0.02]
        )[0]
        
        # Communication preferences
        reminder_sent = random.choices([True, False], weights=[0.90, 0.10])[0] if days_offset < 0 else True
        reminder_method = random.choice(['SMS', 'Email', 'Phone', 'Letter']) if reminder_sent else None
        
        appointments.append({
            'appointment_id': f'APT{i+1:08d}',
            'nhs_number': nhs_number,
            'booking_date': booking_date.strftime('%Y-%m-%d %H:%M:%S'),
            'appointment_date': appointment_datetime.strftime('%Y-%m-%d'),
            'appointment_time': appointment_datetime.strftime('%H:%M'),
            'appointment_datetime': appointment_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'appointment_type': appointment_type,
            'department': department,
            'specialty': specialty,
            'clinician_id': clinician_id,
            'clinician_name': clinician_name,
            'duration_minutes': duration_minutes,
            'priority': priority,
            'attendance_status': attendance_status,
            'actual_arrival_time': actual_arrival_time.strftime('%Y-%m-%d %H:%M:%S') if actual_arrival_time else None,
            'actual_start_time': actual_start_time.strftime('%Y-%m-%d %H:%M:%S') if actual_start_time else None,
            'actual_end_time': actual_end_time.strftime('%Y-%m-%d %H:%M:%S') if actual_end_time else None,
            'wait_time_minutes': wait_time_minutes,
            'cancellation_reason': cancellation_reason,
            'reminder_sent': reminder_sent,
            'reminder_method': reminder_method,
            'location': random.choice(['Main Hospital', 'Community Clinic', 'Health Centre']),
            'room_number': f'{random.choice(["A", "B", "C"])}{random.randint(1, 20)}',
            'created_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    df = pd.DataFrame(appointments)
    
    print(f"\n✅ Generated {len(df):,} appointments")
    print(f"   Appointment types:")
    print(df['appointment_type'].value_counts())
    print(f"\n   Attendance status:")
    print(df['attendance_status'].value_counts())
    print(f"\n   Average wait time (attended): {df[df['attendance_status']=='Attended']['wait_time_minutes'].mean():.1f} minutes")
    
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("APPOINTMENTS DATA GENERATOR")
    print("=" * 60)
    
    df_appointments = generate_appointments_data(num_appointments=120000)
    
    output_file = 'data/sources/appointments/appointments.csv'
    df_appointments.to_csv(output_file, index=False)
    
    print(f"\n💾 Data saved to: {output_file}")
    print(f"   File size: {df_appointments.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    print(f"\n📊 Sample appointments:")
    print(df_appointments.head(3).to_string())
    
    print("\n" + "=" * 60)
    print("APPOINTMENTS DATA GENERATION COMPLETE!")
    print("=" * 60)
