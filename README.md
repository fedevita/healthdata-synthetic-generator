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

- ehr: dati clinici (pazienti, ricoveri, diagnosi)
- erp: dati operativi (reparti, personale, assegnazioni)
- iot: dispositivi e misurazioni (dispositivi, parametri_vitali)

### Tabelle e campi

#### reparti (erp/reparti)

Chiave primaria: id_reparto

Campi:

- Identificativi
  - id_reparto: identificatore stringa del reparto. Tipo: VARCHAR.
- Descrittivi
  - nome_reparto: etichetta del reparto (categorico). Tipo: VARCHAR.
  - specialita: specialita clinica (categorico). Valori nel seed: Cardiologia, Neurologia, Oncologia, Pediatria, Pronto Soccorso, Terapia Intensiva, Ortopedia. Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE reparti (
  id_reparto VARCHAR COMMENT 'Identificatore del reparto',
  nome_reparto VARCHAR COMMENT 'Nome del reparto',
  specialita VARCHAR COMMENT 'Specialita del reparto',
  CONSTRAINT pk_reparti PRIMARY KEY (id_reparto)
)
COMMENT = 'Reparti ospedalieri';
```

#### pazienti (ehr/pazienti)

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
CREATE OR REPLACE TABLE pazienti (
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
  CONSTRAINT pk_pazienti PRIMARY KEY (id_paziente),
  CONSTRAINT ck_pazienti_sesso CHECK (sesso IN ('F', 'M')),
  CONSTRAINT ck_pazienti_stato_civile CHECK (stato_civile IN ('celibe/nubile', 'sposato/a', 'divorziato/a', 'vedovo/a')),
  CONSTRAINT ck_pazienti_lingua_primaria CHECK (lingua_primaria IN ('it')),
  CONSTRAINT ck_pazienti_piano_assicurativo CHECK (piano_assicurativo IN ('basic', 'standard', 'premium')),
  CONSTRAINT ck_pazienti_gruppo_sanguigno CHECK (gruppo_sanguigno IN ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'))
)
COMMENT = 'Anagrafica pazienti';
```

#### personale (erp/personaleersonale)

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
CREATE OR REPLACE TABLE personale (
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
  CONSTRAINT pk_personale PRIMARY KEY (id_staff),
  CONSTRAINT ck_personale_tipo_impiego CHECK (tipo_impiego IN ('Tempo pieno', 'Part-time', 'Contratto'))
)
COMMENT = 'Anagrafica staff';
```

#### assegnazioni (erp/assegnazioni)

Chiave primaria: id_assegnazione

Campi:

- Identificativi
  - id_assegnazione: identificatore stringa dell'assegnazione. Tipo: VARCHAR.
- Relazioni
  - id_staff: chiave esterna verso personale.id_staff. Tipo: VARCHAR.
  - id_reparto: chiave esterna verso reparti.id_reparto. Tipo: VARCHAR.
- Turno
  - turno: valore categorico (Giorno, Notte, Sera nel seed). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE assegnazioni (
  id_assegnazione VARCHAR COMMENT 'Identificatore assegnazione',
  id_staff VARCHAR COMMENT 'Riferimento staff',
  id_reparto VARCHAR COMMENT 'Riferimento reparto',
  turno VARCHAR COMMENT 'Turno',
  CONSTRAINT pk_assegnazioni PRIMARY KEY (id_assegnazione),
  CONSTRAINT fk_assegnazioni_personale FOREIGN KEY (id_staff) REFERENCES personale(id_staff),
  CONSTRAINT fk_assegnazioni_reparti FOREIGN KEY (id_reparto) REFERENCES reparti(id_reparto),
  CONSTRAINT ck_assegnazioni_turno CHECK (turno IN ('Giorno', 'Notte', 'Sera'))
)
COMMENT = 'Assegnazioni del personale ai reparti';
```

#### dispositivi (iot/dispositivi)

Chiave primaria: id_dispositivo

Campi:

- Identificativi
  - id_dispositivo: identificatore stringa del dispositivo. Tipo: VARCHAR.
  - numero_serie: numero di serie. Tipo: VARCHAR.
- Relazioni
  - id_reparto: chiave esterna verso reparti.id_reparto. Tipo: VARCHAR.
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
CREATE OR REPLACE TABLE dispositivi (
  id_dispositivo VARCHAR COMMENT 'Identificatore dispositivo',
  id_reparto VARCHAR COMMENT 'Riferimento reparto',
  tipo_dispositivo VARCHAR COMMENT 'Tipo dispositivo',
  produttore VARCHAR COMMENT 'Produttore',
  modello VARCHAR COMMENT 'Modello',
  numero_serie VARCHAR COMMENT 'Numero di serie',
  stato VARCHAR COMMENT 'Stato operativo',
  data_acquisto DATE COMMENT 'Data acquisto',
  data_ultima_calibrazione DATE COMMENT 'Ultima calibrazione',
  CONSTRAINT pk_dispositivi PRIMARY KEY (id_dispositivo),
  CONSTRAINT fk_dispositivi_reparti FOREIGN KEY (id_reparto) REFERENCES reparti(id_reparto),
  CONSTRAINT ck_dispositivi_tipo_dispositivo CHECK (tipo_dispositivo IN ('ECG', 'Pulsossimetro', 'Sfigmomanometro', 'Termometro')),
  CONSTRAINT ck_dispositivi_stato CHECK (stato IN ('Attivo', 'Manutenzione', 'Ritirato')),
  CONSTRAINT ck_dispositivi_calibrazione CHECK (data_ultima_calibrazione >= data_acquisto)
)
COMMENT = 'Dispositivi IoT';
```

#### ricoveri (ehr/ricoveri)

Chiave primaria: id_ricovero

Campi:

- Identificativi
  - id_ricovero: identificatore stringa del ricovero. Tipo: VARCHAR.
- Relazioni
  - id_paziente: chiave esterna verso pazienti.id_paziente. Tipo: VARCHAR.
  - id_reparto: chiave esterna verso reparti.id_reparto. Tipo: VARCHAR.
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
CREATE OR REPLACE TABLE ricoveri (
  id_ricovero VARCHAR COMMENT 'Identificatore ricovero',
  id_paziente VARCHAR COMMENT 'Riferimento paziente',
  id_reparto VARCHAR COMMENT 'Riferimento reparto',
  data_ricovero TIMESTAMP_NTZ COMMENT 'Data e ora ricovero',
  data_dimissione TIMESTAMP_NTZ COMMENT 'Data e ora dimissione',
  durata_degenza_giorni NUMBER(3,0) COMMENT 'Durata degenza in giorni',
  tipo_ricovero VARCHAR COMMENT 'Tipo di ricovero',
  provenienza_ricovero VARCHAR COMMENT 'Provenienza',
  esito_dimissione VARCHAR COMMENT 'Esito dimissione',
  CONSTRAINT pk_ricoveri PRIMARY KEY (id_ricovero),
  CONSTRAINT fk_ricoveri_pazienti FOREIGN KEY (id_paziente) REFERENCES pazienti(id_paziente),
  CONSTRAINT fk_ricoveri_reparti FOREIGN KEY (id_reparto) REFERENCES reparti(id_reparto),
  CONSTRAINT ck_ricoveri_tipo CHECK (tipo_ricovero IN ('Emergenza', 'Elettivo', 'Urgente')),
  CONSTRAINT ck_ricoveri_provenienza CHECK (provenienza_ricovero IN ('PS', 'Invio', 'Trasferimento')),
  CONSTRAINT ck_ricoveri_esito CHECK (esito_dimissione IN ('Domicilio', 'Trasferimento', 'Riabilitazione', 'Deceduto')),
  CONSTRAINT ck_ricoveri_degenza CHECK (durata_degenza_giorni BETWEEN 1 AND 30),
  CONSTRAINT ck_ricoveri_date CHECK (data_dimissione >= data_ricovero)
)
COMMENT = 'Ricoveri';
```

#### diagnosi (ehr/diagnosi)

Chiave primaria: id_diagnosi

Campi:

- Identificativi
  - id_diagnosi: identificatore stringa della diagnosi. Tipo: VARCHAR.
- Relazioni
  - id_ricovero: chiave esterna verso ricoveri.id_ricovero. Tipo: VARCHAR.
- Codifica clinica
  - codice_icd10: codice diagnostico categorico (I10, E11, J18, K21, M54, N39 nel seed). Tipo: VARCHAR.
  - gravita: valore categorico (bassa, media, alta). Tipo: VARCHAR.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE diagnosi (
  id_diagnosi VARCHAR COMMENT 'Identificatore diagnosi',
  id_ricovero VARCHAR COMMENT 'Riferimento ricovero',
  codice_icd10 VARCHAR COMMENT 'Codice ICD10',
  gravita VARCHAR COMMENT 'Gravita',
  CONSTRAINT pk_diagnosi PRIMARY KEY (id_diagnosi),
  CONSTRAINT fk_diagnosi_ricoveri FOREIGN KEY (id_ricovero) REFERENCES ricoveri(id_ricovero),
  CONSTRAINT ck_diagnosi_gravita CHECK (gravita IN ('bassa', 'media', 'alta'))
)
COMMENT = 'Diagnosi associate ai ricoveri';
```

#### parametri_vitali (iot/parametri_vitali)

Chiave primaria: id_misurazione

Campi:

- Identificativi
  - id_misurazione: identificatore stringa della misurazione. Tipo: VARCHAR.
- Relazioni
  - id_paziente: chiave esterna verso pazienti.id_paziente. Tipo: VARCHAR.
  - id_dispositivo: chiave esterna verso dispositivi.id_dispositivo. Tipo: VARCHAR.
- Timestamp
  - data_misurazione: datetime della misurazione. Tipo: TIMESTAMP_NTZ.
- Parametri vitali (Compilati condizionalmente in base al tipo di dispositivo)
  - frequenza_cardiaca: valore numerico. Tipo: NUMBER(3,0).
  - saturazione_ossigeno: valore numerico. Tipo: NUMBER(3,0).
  - pressione_sistolica: valore numerico. Tipo: NUMBER(3,0).
  - pressione_diastolica: valore numerico. Tipo: NUMBER(3,0).
  - temperatura_c: temperatura corporea in Celsius. Tipo: FLOAT.
  - frequenza_respiratoria: atti respiratori al minuto. Tipo: NUMBER(3,0).
  - glicemia_mg_dl: glicemia in mg/dL. Tipo: NUMBER(3,0).

**Nota**: I campi dei parametri vitali sono popolati solo se il dispositivo associato supporta quella misurazione (es. Termometro popola solo `temperatura_c`). Gli altri campi saranno `NULL`.

Snippet SQL (Snowflake):

```sql
CREATE OR REPLACE TABLE parametri_vitali (
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
  CONSTRAINT pk_parametri_vitali PRIMARY KEY (id_misurazione),
  CONSTRAINT fk_parametri_vitali_pazienti FOREIGN KEY (id_paziente) REFERENCES pazienti(id_paziente),
  CONSTRAINT fk_parametri_vitali_dispositivi FOREIGN KEY (id_dispositivo) REFERENCES dispositivi(id_dispositivo)
)
COMMENT = 'Misurazioni vitali sparse';
```

### Relazioni

I metadati definiscono le seguenti relazioni (genitore -> figlio):

- reparti.id_reparto -> assegnazioni.id_reparto
- reparti.id_reparto -> dispositivi.id_reparto
- reparti.id_reparto -> ricoveri.id_reparto
- pazienti.id_paziente -> ricoveri.id_paziente
- pazienti.id_paziente -> parametri_vitali.id_paziente
- personale.id_staff -> assegnazioni.id_staff
- dispositivi.id_dispositivo -> parametri_vitali.id_dispositivo
- ricoveri.id_ricovero -> diagnosi.id_ricovero

Queste relazioni sono applicate nella pipeline di campionamento sintetico e validate nei test (integrita delle chiavi esterne e controlli di dominio di base).
