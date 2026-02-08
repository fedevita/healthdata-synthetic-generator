# healthdata-synthetic-generator

Minimal project for generating synthetic healthcare datasets in Python using SDV.

## Prerequisites

- Python 3.10+ recommended
- Virtual environment tool (venv/conda) if desired

## Run

```bash
python -m healthdata_synthetic_generator
```

## Output

The script will produce synthetic healthcare datasets (e.g., CSV or Parquet files).

## Data model

The generator produces a multi-table dataset organized in three domains:

- ehr: clinical data (patients, admissions, diagnoses)
- erp: operational data (wards, staff, staff_assignments)
- iot: device and measurements (devices, vital_signs)

### Tables and fields

#### wards (erp/wards)

Primary key: ward_id

Fields:

- ward_id: string identifier for the ward.
- ward_name: ward label (categorical).
- specialty: clinical specialty (categorical). Values in the seed set include Cardiology, Neurology, Oncology, Pediatrics, Emergency, ICU, Orthopedics.

#### patients (ehr/patients)

Primary key: patient_id

Fields:

- patient_id: string identifier for the patient.
- sex: categorical value (F or M).
- birth_date: date value in YYYY-MM-DD format.
- city: city label (categorical, treated as PII in metadata).

#### staff (erp/staff)

Primary key: staff_id

Fields:

- staff_id: string identifier for the staff member.
- role: categorical role (e.g., Nurse, Doctor, Technician, Therapist in the seed set).
- hire_date: date value in YYYY-MM-DD format.

#### staff_assignments (erp/staff_assignments)

Primary key: assignment_id

Fields:

- assignment_id: string identifier for the assignment record.
- staff_id: foreign key to staff.staff_id.
- ward_id: foreign key to wards.ward_id.
- shift: categorical value (Day, Night, Evening in the seed set).

#### devices (iot/devices)

Primary key: device_id

Fields:

- device_id: string identifier for the device.
- ward_id: foreign key to wards.ward_id.
- device_type: categorical value (ECG, PulseOx, BP Monitor, Thermometer in the seed set).

#### admissions (ehr/admissions)

Primary key: admission_id

Fields:

- admission_id: string identifier for the admission.
- patient_id: foreign key to patients.patient_id.
- ward_id: foreign key to wards.ward_id.
- admit_ts: datetime for admission timestamp.
- discharge_ts: datetime for discharge timestamp.

#### diagnoses (ehr/diagnoses)

Primary key: diagnosis_id

Fields:

- diagnosis_id: string identifier for the diagnosis.
- admission_id: foreign key to admissions.admission_id.
- icd10_code: categorical diagnosis code (I10, E11, J18, K21, M54, N39 in the seed set).
- severity: categorical value (low, medium, high).

#### vital_signs (iot/vital_signs)

Primary key: measurement_id

Fields:

- measurement_id: string identifier for the measurement.
- patient_id: foreign key to patients.patient_id.
- device_id: foreign key to devices.device_id.
- measured_at: datetime for measurement timestamp.
- heart_rate: numerical value.
- spo2: numerical value represented as categorical in metadata.
- systolic_bp: numerical value.
- diastolic_bp: numerical value.

### Relationships

The metadata defines the following relationships (parent -> child):

- wards.ward_id -> staff_assignments.ward_id
- wards.ward_id -> devices.ward_id
- wards.ward_id -> admissions.ward_id
- patients.patient_id -> admissions.patient_id
- patients.patient_id -> vital_signs.patient_id
- staff.staff_id -> staff_assignments.staff_id
- devices.device_id -> vital_signs.device_id
- admissions.admission_id -> diagnoses.admission_id

These relationships are enforced in the synthetic sampling pipeline and validated in tests (foreign key integrity and basic domain checks).
