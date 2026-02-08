# healthdata-synthetic-generator

Progetto minimale per generare dataset sanitari sintetici in Python usando SDV.

## Prerequisiti

- Python 3.10+ consigliato
- Strumento per ambienti virtuali (venv/conda) se desiderato

## Avvio

```bash
python -m healthdata_synthetic_generator
```

## Output

Lo script produrra dataset sanitari sintetici (ad esempio file CSV o Parquet).

## Modello dati

Il generatore produce un dataset multi-tabella organizzato in tre domini:

- ehr: dati clinici (patients, admissions, diagnoses)
- erp: dati operativi (wards, staff, staff_assignments)
- iot: dispositivi e misurazioni (devices, vital_signs)

### Tabelle e campi

#### wards (erp/wards)

Chiave primaria: ward_id

Campi:

- ward_id: identificatore stringa del reparto.
- ward_name: etichetta del reparto (categorico).
- specialty: specialita clinica (categorico). Valori nel seed: Cardiology, Neurology, Oncology, Pediatrics, Emergency, ICU, Orthopedics.

#### patients (ehr/patients)

Chiave primaria: patient_id

Campi:

- patient_id: identificatore stringa del paziente.
- first_name: nome.
- last_name: cognome.
- sex: valore categorico (F o M).
- birth_date: data in formato YYYY-MM-DD.
- city: citta (categorico, trattato come PII nei metadati).
- address: indirizzo stradale.
- postal_code: CAP.
- country: paese (Italy nel seed).
- email: indirizzo email.
- phone: numero di telefono.
- national_id: identificativo nazionale sintetico.
- marital_status: valore categorico (single, married, divorced, widowed).
- primary_language: codice lingua (it, en, es, fr, de).
- insurance_provider: nome del provider assicurativo.
- insurance_plan: valore categorico (basic, standard, premium).
- insurance_id: identificativo della polizza.
- emergency_contact_name: nome completo del contatto di emergenza.
- emergency_contact_phone: numero del contatto di emergenza.
- height_cm: altezza in centimetri (intero).
- weight_kg: peso in chilogrammi (intero).
- blood_type: gruppo sanguigno (A+, A-, B+, B-, AB+, AB-, O+, O-).

#### staff (erp/staff)

Chiave primaria: staff_id

Campi:

- staff_id: identificatore stringa del membro dello staff.
- first_name: nome.
- last_name: cognome.
- role: ruolo categorico (ad esempio Nurse, Doctor, Technician, Therapist nel seed).
- department: reparto/area (allineato alle specialita dei reparti).
- employment_type: valore categorico (Full-time, Part-time, Contractor).
- email: email dello staff.
- phone: telefono dello staff.
- license_id: identificativo professionale sintetico.
- hire_date: data in formato YYYY-MM-DD.

#### staff_assignments (erp/staff_assignments)

Chiave primaria: assignment_id

Campi:

- assignment_id: identificatore stringa dell'assegnazione.
- staff_id: chiave esterna verso staff.staff_id.
- ward_id: chiave esterna verso wards.ward_id.
- shift: valore categorico (Day, Night, Evening nel seed).

#### devices (iot/devices)

Chiave primaria: device_id

Campi:

- device_id: identificatore stringa del dispositivo.
- ward_id: chiave esterna verso wards.ward_id.
- device_type: valore categorico (ECG, PulseOx, BP Monitor, Thermometer nel seed).
- manufacturer: produttore del dispositivo.
- model: codice modello.
- serial_number: numero di serie.
- status: valore categorico (Active, Maintenance, Retired).
- purchase_date: data in formato YYYY-MM-DD.
- last_calibration_date: data in formato YYYY-MM-DD.

#### admissions (ehr/admissions)

Chiave primaria: admission_id

Campi:

- admission_id: identificatore stringa del ricovero.
- patient_id: chiave esterna verso patients.patient_id.
- ward_id: chiave esterna verso wards.ward_id.
- admit_ts: datetime del ricovero.
- discharge_ts: datetime della dimissione.
- length_of_stay_days: durata della degenza in giorni (intero).
- admission_type: valore categorico (Emergency, Elective, Urgent).
- admission_source: valore categorico (ER, Referral, Transfer).
- discharge_status: valore categorico (Home, Transfer, Rehab, Deceased).

#### diagnoses (ehr/diagnoses)

Chiave primaria: diagnosis_id

Campi:

- diagnosis_id: identificatore stringa della diagnosi.
- admission_id: chiave esterna verso admissions.admission_id.
- icd10_code: codice diagnostico categorico (I10, E11, J18, K21, M54, N39 nel seed).
- severity: valore categorico (low, medium, high).

#### vital_signs (iot/vital_signs)

Chiave primaria: measurement_id

Campi:

- measurement_id: identificatore stringa della misurazione.
- patient_id: chiave esterna verso patients.patient_id.
- device_id: chiave esterna verso devices.device_id.
- measured_at: datetime della misurazione.
- heart_rate: valore numerico.
- spo2: valore numerico rappresentato come categorico nei metadati.
- systolic_bp: valore numerico.
- diastolic_bp: valore numerico.
- temperature_c: temperatura corporea in Celsius.
- respiratory_rate: atti respiratori al minuto.
- glucose_mg_dl: glicemia in mg/dL.

### Relazioni

I metadati definiscono le seguenti relazioni (genitore -> figlio):

- wards.ward_id -> staff_assignments.ward_id
- wards.ward_id -> devices.ward_id
- wards.ward_id -> admissions.ward_id
- patients.patient_id -> admissions.patient_id
- patients.patient_id -> vital_signs.patient_id
- staff.staff_id -> staff_assignments.staff_id
- devices.device_id -> vital_signs.device_id
- admissions.admission_id -> diagnoses.admission_id

Queste relazioni sono applicate nella pipeline di campionamento sintetico e validate nei test (integrita delle chiavi esterne e controlli di dominio di base).
