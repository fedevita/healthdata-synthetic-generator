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

- Identificativi
	- ward_id: identificatore stringa del reparto. Tipo: VARCHAR.
- Descrittivi
	- ward_name: etichetta del reparto (categorico). Tipo: VARCHAR.
	- specialty: specialita clinica (categorico). Valori nel seed: Cardiologia, Neurologia, Oncologia, Pediatria, Pronto Soccorso, Terapia Intensiva, Ortopedia. Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE wards (
	ward_id VARCHAR COMMENT 'Identificatore del reparto',
	ward_name VARCHAR COMMENT 'Nome del reparto',
	specialty VARCHAR COMMENT 'Specialita del reparto',
	CONSTRAINT pk_wards PRIMARY KEY (ward_id)
)
COMMENT = 'Reparti ospedalieri';
```

#### patients (ehr/patients)

Chiave primaria: patient_id

Campi:

- Identificativi
	- patient_id: identificatore stringa del paziente. Tipo: VARCHAR.
	- national_id: identificativo nazionale sintetico. Tipo: VARCHAR.
- Anagrafica
	- first_name: nome. Tipo: VARCHAR.
	- last_name: cognome. Tipo: VARCHAR.
	- sex: valore categorico (F o M). Tipo: VARCHAR.
	- birth_date: data in formato YYYY-MM-DD. Tipo: DATE.
	- marital_status: valore categorico (single, married, divorced, widowed). Tipo: VARCHAR.
	- primary_language: codice lingua (it, en, es, fr, de). Tipo: VARCHAR.
	- blood_type: gruppo sanguigno (A+, A-, B+, B-, AB+, AB-, O+, O-). Tipo: VARCHAR.
- Contatto e indirizzo
	- city: citta (categorico, trattato come PII nei metadati). Tipo: VARCHAR.
	- address: indirizzo stradale. Tipo: VARCHAR.
	- postal_code: CAP. Tipo: VARCHAR.
	- country: paese (Italia nel seed). Tipo: VARCHAR.
	- email: indirizzo email. Tipo: VARCHAR.
	- phone: numero di telefono. Tipo: VARCHAR.
- Assicurazione
	- insurance_provider: nome del provider assicurativo. Tipo: VARCHAR.
	- insurance_plan: valore categorico (basic, standard, premium). Tipo: VARCHAR.
	- insurance_id: identificativo della polizza. Tipo: VARCHAR.
- Contatto di emergenza
	- emergency_contact_name: nome completo del contatto di emergenza. Tipo: VARCHAR.
	- emergency_contact_phone: numero del contatto di emergenza. Tipo: VARCHAR.
- Misure fisiche
	- height_cm: altezza in centimetri (intero). Tipo: NUMBER(3,0).
	- weight_kg: peso in chilogrammi (intero). Tipo: NUMBER(3,0).

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE patients (
	patient_id VARCHAR COMMENT 'Identificatore paziente',
	first_name VARCHAR COMMENT 'Nome',
	last_name VARCHAR COMMENT 'Cognome',
	sex VARCHAR COMMENT 'Sesso biologico',
	birth_date DATE COMMENT 'Data di nascita',
	city VARCHAR COMMENT 'Citta di residenza',
	address VARCHAR COMMENT 'Indirizzo',
	postal_code VARCHAR COMMENT 'CAP',
	country VARCHAR COMMENT 'Paese',
	email VARCHAR COMMENT 'Email',
	phone VARCHAR COMMENT 'Telefono',
	national_id VARCHAR COMMENT 'Identificativo nazionale sintetico',
	marital_status VARCHAR COMMENT 'Stato civile',
	primary_language VARCHAR COMMENT 'Lingua primaria',
	insurance_provider VARCHAR COMMENT 'Provider assicurativo',
	insurance_plan VARCHAR COMMENT 'Piano assicurativo',
	insurance_id VARCHAR COMMENT 'Identificativo polizza',
	emergency_contact_name VARCHAR COMMENT 'Contatto di emergenza - nome',
	emergency_contact_phone VARCHAR COMMENT 'Contatto di emergenza - telefono',
	height_cm NUMBER(3,0) COMMENT 'Altezza in cm',
	weight_kg NUMBER(3,0) COMMENT 'Peso in kg',
	blood_type VARCHAR COMMENT 'Gruppo sanguigno',
	CONSTRAINT pk_patients PRIMARY KEY (patient_id),
	CONSTRAINT ck_patients_sex CHECK (sex IN ('F', 'M')),
	CONSTRAINT ck_patients_marital_status CHECK (marital_status IN ('single', 'married', 'divorced', 'widowed')),
	CONSTRAINT ck_patients_primary_language CHECK (primary_language IN ('it', 'en', 'es', 'fr', 'de')),
	CONSTRAINT ck_patients_insurance_plan CHECK (insurance_plan IN ('basic', 'standard', 'premium')),
	CONSTRAINT ck_patients_blood_type CHECK (blood_type IN ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'))
)
COMMENT = 'Anagrafica pazienti';
```

#### staff (erp/staff)

Chiave primaria: staff_id

Campi:

- Identificativi
	- staff_id: identificatore stringa del membro dello staff. Tipo: VARCHAR.
	- license_id: identificativo professionale sintetico. Tipo: VARCHAR.
- Anagrafica
	- first_name: nome. Tipo: VARCHAR.
	- last_name: cognome. Tipo: VARCHAR.
- Ruolo e impiego
	- role: ruolo categorico (ad esempio Infermiere, Medico, Tecnico, Terapista nel seed). Tipo: VARCHAR.
	- department: reparto/area (allineato alle specialita dei reparti). Tipo: VARCHAR.
	- employment_type: valore categorico (Tempo pieno, Part-time, Contratto). Tipo: VARCHAR.
	- hire_date: data in formato YYYY-MM-DD. Tipo: DATE.
- Contatto
	- email: email dello staff. Tipo: VARCHAR.
	- phone: telefono dello staff. Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE staff (
	staff_id VARCHAR COMMENT 'Identificatore staff',
	first_name VARCHAR COMMENT 'Nome',
	last_name VARCHAR COMMENT 'Cognome',
	role VARCHAR COMMENT 'Ruolo',
	department VARCHAR COMMENT 'Reparto o area',
	employment_type VARCHAR COMMENT 'Tipo di impiego',
	email VARCHAR COMMENT 'Email',
	phone VARCHAR COMMENT 'Telefono',
	license_id VARCHAR COMMENT 'Identificativo professionale',
	hire_date DATE COMMENT 'Data assunzione',
	CONSTRAINT pk_staff PRIMARY KEY (staff_id),
	CONSTRAINT ck_staff_employment_type CHECK (employment_type IN ('Tempo pieno', 'Part-time', 'Contratto'))
)
COMMENT = 'Anagrafica staff';
```

#### staff_assignments (erp/staff_assignments)

Chiave primaria: assignment_id

Campi:

- Identificativi
	- assignment_id: identificatore stringa dell'assegnazione. Tipo: VARCHAR.
- Relazioni
	- staff_id: chiave esterna verso staff.staff_id. Tipo: VARCHAR.
	- ward_id: chiave esterna verso wards.ward_id. Tipo: VARCHAR.
- Turno
	- shift: valore categorico (Giorno, Notte, Sera nel seed). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE staff_assignments (
	assignment_id VARCHAR COMMENT 'Identificatore assegnazione',
	staff_id VARCHAR COMMENT 'Riferimento staff',
	ward_id VARCHAR COMMENT 'Riferimento reparto',
	shift VARCHAR COMMENT 'Turno',
	CONSTRAINT pk_staff_assignments PRIMARY KEY (assignment_id),
	CONSTRAINT fk_staff_assignments_staff FOREIGN KEY (staff_id) REFERENCES staff(staff_id),
	CONSTRAINT fk_staff_assignments_wards FOREIGN KEY (ward_id) REFERENCES wards(ward_id),
	CONSTRAINT ck_staff_assignments_shift CHECK (shift IN ('Giorno', 'Notte', 'Sera'))
)
COMMENT = 'Assegnazioni del personale ai reparti';
```

#### devices (iot/devices)

Chiave primaria: device_id

Campi:

- Identificativi
	- device_id: identificatore stringa del dispositivo. Tipo: VARCHAR.
	- serial_number: numero di serie. Tipo: VARCHAR.
- Relazioni
	- ward_id: chiave esterna verso wards.ward_id. Tipo: VARCHAR.
- Descrittivi
	- device_type: valore categorico (ECG, Pulsossimetro, Sfigmomanometro, Termometro nel seed). Tipo: VARCHAR.
	- manufacturer: produttore del dispositivo. Tipo: VARCHAR.
	- model: codice modello. Tipo: VARCHAR.
	- status: valore categorico (Attivo, Manutenzione, Ritirato). Tipo: VARCHAR.
- Date di ciclo vita
	- purchase_date: data in formato YYYY-MM-DD. Tipo: DATE.
	- last_calibration_date: data in formato YYYY-MM-DD. Tipo: DATE.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE devices (
	device_id VARCHAR COMMENT 'Identificatore dispositivo',
	ward_id VARCHAR COMMENT 'Riferimento reparto',
	device_type VARCHAR COMMENT 'Tipo dispositivo',
	manufacturer VARCHAR COMMENT 'Produttore',
	model VARCHAR COMMENT 'Modello',
	serial_number VARCHAR COMMENT 'Numero di serie',
	status VARCHAR COMMENT 'Stato operativo',
	purchase_date DATE COMMENT 'Data acquisto',
	last_calibration_date DATE COMMENT 'Ultima calibrazione',
	CONSTRAINT pk_devices PRIMARY KEY (device_id),
	CONSTRAINT fk_devices_wards FOREIGN KEY (ward_id) REFERENCES wards(ward_id),
	CONSTRAINT ck_devices_device_type CHECK (device_type IN ('ECG', 'Pulsossimetro', 'Sfigmomanometro', 'Termometro')),
	CONSTRAINT ck_devices_status CHECK (status IN ('Attivo', 'Manutenzione', 'Ritirato')),
	CONSTRAINT ck_devices_calibration CHECK (last_calibration_date >= purchase_date)
)
COMMENT = 'Dispositivi IoT';
```

#### admissions (ehr/admissions)

Chiave primaria: admission_id

Campi:

- Identificativi
	- admission_id: identificatore stringa del ricovero. Tipo: VARCHAR.
- Relazioni
	- patient_id: chiave esterna verso patients.patient_id. Tipo: VARCHAR.
	- ward_id: chiave esterna verso wards.ward_id. Tipo: VARCHAR.
- Date e durata
	- admit_ts: datetime del ricovero. Tipo: TIMESTAMP_NTZ.
	- discharge_ts: datetime della dimissione. Tipo: TIMESTAMP_NTZ.
	- length_of_stay_days: durata della degenza in giorni (intero). Tipo: NUMBER(3,0).
- Classificazioni
	- admission_type: valore categorico (Emergenza, Elettivo, Urgente). Tipo: VARCHAR.
	- admission_source: valore categorico (PS, Invio, Trasferimento). Tipo: VARCHAR.
	- discharge_status: valore categorico (Domicilio, Trasferimento, Riabilitazione, Deceduto). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE admissions (
	admission_id VARCHAR COMMENT 'Identificatore ricovero',
	patient_id VARCHAR COMMENT 'Riferimento paziente',
	ward_id VARCHAR COMMENT 'Riferimento reparto',
	admit_ts TIMESTAMP_NTZ COMMENT 'Data e ora ricovero',
	discharge_ts TIMESTAMP_NTZ COMMENT 'Data e ora dimissione',
	length_of_stay_days NUMBER(3,0) COMMENT 'Durata degenza in giorni',
	admission_type VARCHAR COMMENT 'Tipo di ricovero',
	admission_source VARCHAR COMMENT 'Provenienza',
	discharge_status VARCHAR COMMENT 'Esito dimissione',
	CONSTRAINT pk_admissions PRIMARY KEY (admission_id),
	CONSTRAINT fk_admissions_patients FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
	CONSTRAINT fk_admissions_wards FOREIGN KEY (ward_id) REFERENCES wards(ward_id),
	CONSTRAINT ck_admissions_type CHECK (admission_type IN ('Emergenza', 'Elettivo', 'Urgente')),
	CONSTRAINT ck_admissions_source CHECK (admission_source IN ('PS', 'Invio', 'Trasferimento')),
	CONSTRAINT ck_admissions_discharge CHECK (discharge_status IN ('Domicilio', 'Trasferimento', 'Riabilitazione', 'Deceduto')),
	CONSTRAINT ck_admissions_los CHECK (length_of_stay_days BETWEEN 1 AND 30),
	CONSTRAINT ck_admissions_dates CHECK (discharge_ts >= admit_ts)
)
COMMENT = 'Ricoveri';
```

#### diagnoses (ehr/diagnoses)

Chiave primaria: diagnosis_id

Campi:

- Identificativi
	- diagnosis_id: identificatore stringa della diagnosi. Tipo: VARCHAR.
- Relazioni
	- admission_id: chiave esterna verso admissions.admission_id. Tipo: VARCHAR.
- Codifica clinica
	- icd10_code: codice diagnostico categorico (I10, E11, J18, K21, M54, N39 nel seed). Tipo: VARCHAR.
	- severity: valore categorico (bassa, media, alta). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE diagnoses (
	diagnosis_id VARCHAR COMMENT 'Identificatore diagnosi',
	admission_id VARCHAR COMMENT 'Riferimento ricovero',
	icd10_code VARCHAR COMMENT 'Codice ICD10',
	severity VARCHAR COMMENT 'Gravita',
	CONSTRAINT pk_diagnoses PRIMARY KEY (diagnosis_id),
	CONSTRAINT fk_diagnoses_admissions FOREIGN KEY (admission_id) REFERENCES admissions(admission_id),
	CONSTRAINT ck_diagnoses_severity CHECK (severity IN ('bassa', 'media', 'alta'))
)
COMMENT = 'Diagnosi associate ai ricoveri';
```

#### vital_signs (iot/vital_signs)

Chiave primaria: measurement_id

Campi:

- Identificativi
	- measurement_id: identificatore stringa della misurazione. Tipo: VARCHAR.
- Relazioni
	- patient_id: chiave esterna verso patients.patient_id. Tipo: VARCHAR.
	- device_id: chiave esterna verso devices.device_id. Tipo: VARCHAR.
- Timestamp
	- measured_at: datetime della misurazione. Tipo: TIMESTAMP_NTZ.
- Parametri vitali
	- heart_rate: valore numerico. Tipo: NUMBER(3,0).
	- spo2: valore numerico rappresentato come categorico nei metadati. Tipo: NUMBER(3,0).
	- systolic_bp: valore numerico. Tipo: NUMBER(3,0).
	- diastolic_bp: valore numerico. Tipo: NUMBER(3,0).
	- temperature_c: temperatura corporea in Celsius. Tipo: FLOAT.
	- respiratory_rate: atti respiratori al minuto. Tipo: NUMBER(3,0).
	- glucose_mg_dl: glicemia in mg/dL. Tipo: NUMBER(3,0).

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE vital_signs (
	measurement_id VARCHAR COMMENT 'Identificatore misurazione',
	patient_id VARCHAR COMMENT 'Riferimento paziente',
	device_id VARCHAR COMMENT 'Riferimento dispositivo',
	measured_at TIMESTAMP_NTZ COMMENT 'Data e ora misurazione',
	heart_rate NUMBER(3,0) COMMENT 'Frequenza cardiaca',
	spo2 NUMBER(3,0) COMMENT 'Saturazione ossigeno',
	systolic_bp NUMBER(3,0) COMMENT 'Pressione sistolica',
	diastolic_bp NUMBER(3,0) COMMENT 'Pressione diastolica',
	temperature_c FLOAT COMMENT 'Temperatura corporea in C',
	respiratory_rate NUMBER(3,0) COMMENT 'Atti respiratori/min',
	glucose_mg_dl NUMBER(3,0) COMMENT 'Glicemia mg/dL',
	CONSTRAINT pk_vital_signs PRIMARY KEY (measurement_id),
	CONSTRAINT fk_vital_signs_patients FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
	CONSTRAINT fk_vital_signs_devices FOREIGN KEY (device_id) REFERENCES devices(device_id)
)
COMMENT = 'Misurazioni vitali';
```

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
