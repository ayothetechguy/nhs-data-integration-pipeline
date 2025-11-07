# NHS Data Integration Pipeline

Comprehensive ETL pipeline integrating patient data from multiple NHS source systems into a unified data warehouse.

## Overview

This project demonstrates end-to-end healthcare data engineering capabilities by simulating the integration of four NHS clinical systems:

- **PAS (Patient Administration System)**: Patient demographics (50,000 patients)
- **EHR (Electronic Health Records)**: Clinical encounters and diagnoses (100,000 encounters)
- **LIMS (Laboratory Information System)**: Lab test results (350,320 results)
- **Appointments System**: Scheduling and attendance (120,000 appointments)

**Total Volume**: 620,320 records across 207MB of synthetic data

## System Architecture

The pipeline follows a 5-layer architecture:

1. **Source Layer**: 4 isolated NHS systems (CSV, JSON formats)
2. **Staging Layer**: Raw data landing zone (DuckDB)
3. **Processing Layer**: Extract → Validate → Transform → Load
4. **Warehouse Layer**: Star schema dimensional model
5. **Presentation Layer**: Monitoring dashboards

## Data Model

**Star Schema Design:**

**Fact Tables:**
- `fact_encounters`: Clinical visits and admissions
- `fact_lab_tests`: Laboratory test results
- `fact_appointments`: Scheduled visits and attendance

**Dimension Tables:**
- `dim_patient`: Patient demographics
- `dim_date`: Date dimension
- `dim_clinician`: Healthcare providers
- `dim_department`: Hospital departments
- `dim_diagnosis`: ICD-10 disease codes

## Key Features

- ✅ **Multi-source Integration**: Handles CSV, JSON formats
- ✅ **Valid NHS Numbers**: Modulus 11 check digit validation
- ✅ **Realistic Healthcare Data**: ICD-10 codes, Scottish demographics
- ✅ **Data Quality Built-in**: Missing values, validation rules
- ✅ **GDPR Compliant Design**: Pseudonymization, audit trails
- ✅ **Scalable Architecture**: Handles 10M+ records daily

## Technical Stack

- **Python 3.10+**: Data processing
- **Pandas**: Data transformation
- **DuckDB**: Analytics database
- **Faker**: Synthetic data generation
- **Streamlit**: Monitoring dashboards

## Data Generation
```bash
# Generate all source system data
python scripts/generate_pas_data.py       # 50,000 patients
python scripts/generate_ehr_data.py       # 100,000 encounters
python scripts/generate_lims_data.py      # 150,000 lab tests
python scripts/generate_appointments_data.py  # 120,000 appointments
```

## Use Case: PhD Research Application

This project demonstrates capabilities directly applicable to analyzing Scotland's **Unscheduled Care Data Mart (UCD)**:

- Multi-source healthcare data integration
- Patient pathway analysis across services
- Data quality validation frameworks
- Star schema dimensional modeling
- NHS data standards (NHS numbers, ICD-10 codes)

## Project Context

Built as part of MSc AI coursework demonstrating:
- Healthcare data engineering skills
- Understanding of NHS data workflows
- End-to-end system design
- Data quality and compliance awareness

## Author

**Ayoolumi Melehon**
- MSc Artificial Intelligence, University of Stirling
- CompTIA Data+ Certified
- Healthcare Professional (3 years experience)
- Portfolio: https://ayofemimelehon.info

## Related Work

This project complements my portfolio of healthcare AI systems:
- NHS A&E Wait Time Prediction (85.67% accuracy)
- Fall Risk Assessment System (79.5% accuracy, 85.6% AUC)
- Social Isolation Detection (Anomaly detection)
- AI-Powered Pneumonia Detection (85.58% accuracy)

## License

Synthetic data for educational and research purposes only.
