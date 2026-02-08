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

Chiave primaria: id_reparto

Campi:

- Identificativi
  - id_reparto: identificatore stringa del reparto. Tipo: VARCHAR.
- Descrittivi
  - nome_reparto: etichetta del reparto (categorico). Tipo: VARCHAR.
  - specialita: specialita clinica (categorico). Valori nel seed: Cardiologia, Neurologia, Oncologia, Pediatria, Pronto Soccorso, Terapia Intensiva, Ortopedia. Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE wards (
  id_reparto VARCHAR COMMENT 'Identificatore del reparto',
  nome_reparto VARCHAR COMMENT 'Nome del reparto',
  specialita VARCHAR COMMENT 'Specialita del reparto',
  CONSTRAINT pk_wards PRIMARY KEY (id_reparto)
)
COMMENT = 'Reparti ospedalieri';
```

#### patients (ehr/patients)

Chiave primaria: id_paziente

Campi:

- Identificativi
  - id_paziente: identificatore stringa del paziente. Tipo: VARCHAR.
  - codice_fiscale: identificativo nazionale sintetico. Tipo: VARCHAR.
- Anagrafica
  - nome: nome. Tipo: VARCHAR.
  - cognome: cognome. Tipo: VARCHAR.
  - sesso: valore categorico (F o M). Tipo: VARCHAR.
  - data_nascita: data in formato YYYY-MM-DD. Tipo: DATE.
  - stato_civile: valore categorico (celibe/nubile, sposato/a, divorziato/a, vedovo/a). Tipo: VARCHAR.
  - lingua_primaria: codice lingua (it). Tipo: VARCHAR.
  - gruppo_sanguigno: gruppo sanguigno (A+, A-, B+, B-, AB+, AB-, O+, O-). Tipo: VARCHAR.
- Contatto e indirizzo
  - citta: citta (categorico, trattato come PII nei metadati). Tipo: VARCHAR.
  - indirizzo: indirizzo stradale. Tipo: VARCHAR.
  - cap: CAP. Tipo: VARCHAR.
  - paese: paese (Italia nel seed). Tipo: VARCHAR.
  - email: indirizzo email. Tipo: VARCHAR.
  - telefono: numero di telefono. Tipo: VARCHAR.
- Assicurazione
  - compagnia_assicurativa: nome del provider assicurativo. Tipo: VARCHAR.
  - piano_assicurativo: valore categorico (basic, standard, premium). Tipo: VARCHAR.
  - id_assicurazione: identificativo della polizza. Tipo: VARCHAR.
- Contatto di emergenza
  - contatto_emergenza_nome: nome completo del contatto di emergenza. Tipo: VARCHAR.
  - contatto_emergenza_telefono: numero del contatto di emergenza. Tipo: VARCHAR.
- Misure fisiche
  - altezza_cm: altezza in centimetri (intero). Tipo: NUMBER(3,0).
  - peso_kg: peso in chilogrammi (intero). Tipo: NUMBER(3,0).

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE patients (
  id_paziente VARCHAR COMMENT 'Identificatore paziente',
  nome VARCHAR COMMENT 'Nome',
  cognome VARCHAR COMMENT 'Cognome',
  sesso VARCHAR COMMENT 'Sesso biologico',
  data_nascita DATE COMMENT 'Data di nascita',
  citta VARCHAR COMMENT 'Citta di residenza',
  indirizzo VARCHAR COMMENT 'Indirizzo',
  cap VARCHAR COMMENT 'CAP',
  paese VARCHAR COMMENT 'Paese',
  email VARCHAR COMMENT 'Email',
  telefono VARCHAR COMMENT 'Telefono',
  codice_fiscale VARCHAR COMMENT 'Identificativo nazionale sintetico',
  stato_civile VARCHAR COMMENT 'Stato civile',
  lingua_primaria VARCHAR COMMENT 'Lingua primaria',
  compagnia_assicurativa VARCHAR COMMENT 'Provider assicurativo',
  piano_assicurativo VARCHAR COMMENT 'Piano assicurativo',
  id_assicurazione VARCHAR COMMENT 'Identificativo polizza',
  contatto_emergenza_nome VARCHAR COMMENT 'Contatto di emergenza - nome',
  contatto_emergenza_telefono VARCHAR COMMENT 'Contatto di emergenza - telefono',
  altezza_cm NUMBER(3,0) COMMENT 'Altezza in cm',
  peso_kg NUMBER(3,0) COMMENT 'Peso in kg',
  gruppo_sanguigno VARCHAR COMMENT 'Gruppo sanguigno',
  CONSTRAINT pk_patients PRIMARY KEY (id_paziente),
  CONSTRAINT ck_patients_sesso CHECK (sesso IN ('F', 'M')),
  CONSTRAINT ck_patients_stato_civile CHECK (stato_civile IN ('celibe/nubile', 'sposato/a', 'divorziato/a', 'vedovo/a')),
  CONSTRAINT ck_patients_lingua_primaria CHECK (lingua_primaria IN ('it')),
  CONSTRAINT ck_patients_piano_assicurativo CHECK (piano_assicurativo IN ('basic', 'standard', 'premium')),
  CONSTRAINT ck_patients_gruppo_sanguigno CHECK (gruppo_sanguigno IN ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'))
)
COMMENT = 'Anagrafica pazienti';
```

#### staff (erp/staff)

Chiave primaria: id_staff

Campi:

- Identificativi
  - id_staff: identificatore stringa del membro dello staff. Tipo: VARCHAR.
  - id_licenza: identificativo professionale sintetico. Tipo: VARCHAR.
- Anagrafica
  - nome: nome. Tipo: VARCHAR.
  - cognome: cognome. Tipo: VARCHAR.
- Ruolo e impiego
  - ruolo: ruolo categorico (ad esempio Infermiere, Medico, Tecnico, Terapista nel seed). Tipo: VARCHAR.
  - reparto: reparto/area (allineato alle specialita dei reparti). Tipo: VARCHAR.
  - tipo_impiego: valore categorico (Tempo pieno, Part-time, Contratto). Tipo: VARCHAR.
  - data_assunzione: data in formato YYYY-MM-DD. Tipo: DATE.
- Contatto
  - email: email dello staff. Tipo: VARCHAR.
  - telefono: telefono dello staff. Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE staff (
  id_staff VARCHAR COMMENT 'Identificatore staff',
  nome VARCHAR COMMENT 'Nome',
  cognome VARCHAR COMMENT 'Cognome',
  ruolo VARCHAR COMMENT 'Ruolo',
  reparto VARCHAR COMMENT 'Reparto o area',
  tipo_impiego VARCHAR COMMENT 'Tipo di impiego',
  email VARCHAR COMMENT 'Email',
  telefono VARCHAR COMMENT 'Telefono',
  id_licenza VARCHAR COMMENT 'Identificativo professionale',
  data_assunzione DATE COMMENT 'Data assunzione',
  CONSTRAINT pk_staff PRIMARY KEY (id_staff),
  CONSTRAINT ck_staff_tipo_impiego CHECK (tipo_impiego IN ('Tempo pieno', 'Part-time', 'Contratto'))
)
COMMENT = 'Anagrafica staff';
```

#### staff_assignments (erp/staff_assignments)

Chiave primaria: id_assegnazione

Campi:

- Identificativi
  - id_assegnazione: identificatore stringa dell'assegnazione. Tipo: VARCHAR.
- Relazioni
  - id_staff: chiave esterna verso staff.id_staff. Tipo: VARCHAR.
  - id_reparto: chiave esterna verso wards.id_reparto. Tipo: VARCHAR.
- Turno
  - turno: valore categorico (Giorno, Notte, Sera nel seed). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE staff_assignments (
  id_assegnazione VARCHAR COMMENT 'Identificatore assegnazione',
  id_staff VARCHAR COMMENT 'Riferimento staff',
  id_reparto VARCHAR COMMENT 'Riferimento reparto',
  turno VARCHAR COMMENT 'Turno',
  CONSTRAINT pk_staff_assignments PRIMARY KEY (id_assegnazione),
  CONSTRAINT fk_staff_assignments_staff FOREIGN KEY (id_staff) REFERENCES staff(id_staff),
  CONSTRAINT fk_staff_assignments_wards FOREIGN KEY (id_reparto) REFERENCES wards(id_reparto),
  CONSTRAINT ck_staff_assignments_turno CHECK (turno IN ('Giorno', 'Notte', 'Sera'))
)
COMMENT = 'Assegnazioni del personale ai reparti';
```

#### devices (iot/devices)

Chiave primaria: id_dispositivo

Campi:

- Identificativi
  - id_dispositivo: identificatore stringa del dispositivo. Tipo: VARCHAR.
  - numero_serie: numero di serie. Tipo: VARCHAR.
- Relazioni
  - id_reparto: chiave esterna verso wards.id_reparto. Tipo: VARCHAR.
- Descrittivi
  - tipo_dispositivo: valore categorico (ECG, Pulsossimetro, Sfigmomanometro, Termometro nel seed). Tipo: VARCHAR.
  - produttore: produttore del dispositivo. Tipo: VARCHAR.
  - modello: codice modello. Tipo: VARCHAR.
  - stato: valore categorico (Attivo, Manutenzione, Ritirato). Tipo: VARCHAR.
- Date di ciclo vita
  - data_acquisto: data in formato YYYY-MM-DD. Tipo: DATE.
  - data_ultima_calibrazione: data in formato YYYY-MM-DD. Tipo: DATE.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE devices (
  id_dispositivo VARCHAR COMMENT 'Identificatore dispositivo',
  id_reparto VARCHAR COMMENT 'Riferimento reparto',
  tipo_dispositivo VARCHAR COMMENT 'Tipo dispositivo',
  produttore VARCHAR COMMENT 'Produttore',
  modello VARCHAR COMMENT 'Modello',
  numero_serie VARCHAR COMMENT 'Numero di serie',
  stato VARCHAR COMMENT 'Stato operativo',
  data_acquisto DATE COMMENT 'Data acquisto',
  data_ultima_calibrazione DATE COMMENT 'Ultima calibrazione',
  CONSTRAINT pk_devices PRIMARY KEY (id_dispositivo),
  CONSTRAINT fk_devices_wards FOREIGN KEY (id_reparto) REFERENCES wards(id_reparto),
  CONSTRAINT ck_devices_tipo_dispositivo CHECK (tipo_dispositivo IN ('ECG', 'Pulsossimetro', 'Sfigmomanometro', 'Termometro')),
  CONSTRAINT ck_devices_stato CHECK (stato IN ('Attivo', 'Manutenzione', 'Ritirato')),
  CONSTRAINT ck_devices_calibrazione CHECK (data_ultima_calibrazione >= data_acquisto)
)
COMMENT = 'Dispositivi IoT';
```

#### admissions (ehr/admissions)

Chiave primaria: id_ricovero

Campi:

- Identificativi
  - id_ricovero: identificatore stringa del ricovero. Tipo: VARCHAR.
- Relazioni
  - id_paziente: chiave esterna verso patients.id_paziente. Tipo: VARCHAR.
  - id_reparto: chiave esterna verso wards.id_reparto. Tipo: VARCHAR.
- Date e durata
  - data_ricovero: datetime del ricovero. Tipo: TIMESTAMP_NTZ.
  - data_dimissione: datetime della dimissione. Tipo: TIMESTAMP_NTZ.
  - durata_degenza_giorni: durata della degenza in giorni (intero). Tipo: NUMBER(3,0).
- Classificazioni
  - tipo_ricovero: valore categorico (Emergenza, Elettivo, Urgente). Tipo: VARCHAR.
  - provenienza_ricovero: valore categorico (PS, Invio, Trasferimento). Tipo: VARCHAR.
  - esito_dimissione: valore categorico (Domicilio, Trasferimento, Riabilitazione, Deceduto). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE admissions (
  id_ricovero VARCHAR COMMENT 'Identificatore ricovero',
  id_paziente VARCHAR COMMENT 'Riferimento paziente',
  id_reparto VARCHAR COMMENT 'Riferimento reparto',
  data_ricovero TIMESTAMP_NTZ COMMENT 'Data e ora ricovero',
  data_dimissione TIMESTAMP_NTZ COMMENT 'Data e ora dimissione',
  durata_degenza_giorni NUMBER(3,0) COMMENT 'Durata degenza in giorni',
  tipo_ricovero VARCHAR COMMENT 'Tipo di ricovero',
  provenienza_ricovero VARCHAR COMMENT 'Provenienza',
  esito_dimissione VARCHAR COMMENT 'Esito dimissione',
  CONSTRAINT pk_admissions PRIMARY KEY (id_ricovero),
  CONSTRAINT fk_admissions_patients FOREIGN KEY (id_paziente) REFERENCES patients(id_paziente),
  CONSTRAINT fk_admissions_wards FOREIGN KEY (id_reparto) REFERENCES wards(id_reparto),
  CONSTRAINT ck_admissions_tipo CHECK (tipo_ricovero IN ('Emergenza', 'Elettivo', 'Urgente')),
  CONSTRAINT ck_admissions_provenienza CHECK (provenienza_ricovero IN ('PS', 'Invio', 'Trasferimento')),
  CONSTRAINT ck_admissions_esito CHECK (esito_dimissione IN ('Domicilio', 'Trasferimento', 'Riabilitazione', 'Deceduto')),
  CONSTRAINT ck_admissions_degenza CHECK (durata_degenza_giorni BETWEEN 1 AND 30),
  CONSTRAINT ck_admissions_date CHECK (data_dimissione >= data_ricovero)
)
COMMENT = 'Ricoveri';
```

#### diagnoses (ehr/diagnoses)

Chiave primaria: id_diagnosi

Campi:

- Identificativi
  - id_diagnosi: identificatore stringa della diagnosi. Tipo: VARCHAR.
- Relazioni
  - id_ricovero: chiave esterna verso admissions.id_ricovero. Tipo: VARCHAR.
- Codifica clinica
  - codice_icd10: codice diagnostico categorico (I10, E11, J18, K21, M54, N39 nel seed). Tipo: VARCHAR.
  - gravita: valore categorico (bassa, media, alta). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE diagnoses (
  id_diagnosi VARCHAR COMMENT 'Identificatore diagnosi',
  id_ricovero VARCHAR COMMENT 'Riferimento ricovero',
  codice_icd10 VARCHAR COMMENT 'Codice ICD10',
  gravita VARCHAR COMMENT 'Gravita',
  CONSTRAINT pk_diagnoses PRIMARY KEY (id_diagnosi),
  CONSTRAINT fk_diagnoses_admissions FOREIGN KEY (id_ricovero) REFERENCES admissions(id_ricovero),
  CONSTRAINT ck_diagnoses_gravita CHECK (gravita IN ('bassa', 'media', 'alta'))
)
COMMENT = 'Diagnosi associate ai ricoveri';
```

#### vital_signs (iot/vital_signs)

Chiave primaria: id_misurazione

Campi:

- Identificativi
  - id_misurazione: identificatore stringa della misurazione. Tipo: VARCHAR.
- Relazioni
  - id_paziente: chiave esterna verso patients.id_paziente. Tipo: VARCHAR.
  - id_dispositivo: chiave esterna verso devices.id_dispositivo. Tipo: VARCHAR.
- Timestamp
  - data_misurazione: datetime della misurazione. Tipo: TIMESTAMP_NTZ.
- Parametri vitali
  - frequenza_cardiaca: valore numerico. Tipo: NUMBER(3,0).
  - saturazione_ossigeno: valore numerico rappresentato come categorico nei metadati. Tipo: NUMBER(3,0).
  - pressione_sistolica: valore numerico. Tipo: NUMBER(3,0).
  - pressione_diastolica: valore numerico. Tipo: NUMBER(3,0).
  - temperatura_c: temperatura corporea in Celsius. Tipo: FLOAT.
  - frequenza_respiratoria: atti respiratori al minuto. Tipo: NUMBER(3,0).
  - glicemia_mg_dl: glicemia in mg/dL. Tipo: NUMBER(3,0).

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE vital_signs (
  id_misurazione VARCHAR COMMENT 'Identificatore misurazione',
  id_paziente VARCHAR COMMENT 'Riferimento paziente',
  id_dispositivo VARCHAR COMMENT 'Riferimento dispositivo',
  data_misurazione TIMESTAMP_NTZ COMMENT 'Data e ora misurazione',
  frequenza_cardiaca NUMBER(3,0) COMMENT 'Frequenza cardiaca',
  saturazione_ossigeno NUMBER(3,0) COMMENT 'Saturazione ossigeno',
  pressione_sistolica NUMBER(3,0) COMMENT 'Pressione sistolica',
  pressione_diastolica NUMBER(3,0) COMMENT 'Pressione diastolica',
  temperatura_c FLOAT COMMENT 'Temperatura corporea in C',
  frequenza_respiratoria NUMBER(3,0) COMMENT 'Atti respiratori/min',
  glicemia_mg_dl NUMBER(3,0) COMMENT 'Glicemia mg/dL',
  CONSTRAINT pk_vital_signs PRIMARY KEY (id_misurazione),
  CONSTRAINT fk_vital_signs_patients FOREIGN KEY (id_paziente) REFERENCES patients(id_paziente),
  CONSTRAINT fk_vital_signs_devices FOREIGN KEY (id_dispositivo) REFERENCES devices(id_dispositivo)
)
COMMENT = 'Misurazioni vitali';
```

### Relazioni

I metadati definiscono le seguenti relazioni (genitore -> figlio):

- wards.id_reparto -> staff_assignments.id_reparto
- wards.id_reparto -> devices.id_reparto
- wards.id_reparto -> admissions.id_reparto
- patients.id_paziente -> admissions.id_paziente
- patients.id_paziente -> vital_signs.id_paziente
- staff.id_staff -> staff_assignments.id_staff
- devices.id_dispositivo -> vital_signs.id_dispositivo
- admissions.id_ricovero -> diagnoses.id_ricovero

Queste relazioni sono applicate nella pipeline di campionamento sintetico e validate nei test (integrita delle chiavi esterne e controlli di dominio di base).
