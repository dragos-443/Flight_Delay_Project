# Flight Delay Project - Report Finale

## 1. Introduzione

Questo report documenta lo sviluppo incrementale del progetto sul Flight Delay Dataset 2024, con confronto tra Spark SQL, Spark Core e Hive. MapReduce tramite Python e Hadoop Streaming e previsto come estensione opzionale.

## 2. Dataset

Dataset: Flight Delay Dataset 2024.

Record attesi: oltre 7 milioni.

Formato originale: CSV.

Colonne principali:

- compagnia aerea: `op_unique_carrier`;
- aeroporto di partenza: `origin`;
- aeroporto di destinazione: `dest`;
- mese: `month`;
- ritardo in partenza: `dep_delay`;
- ritardo in arrivo: `arr_delay`;
- cancellazione: `cancelled`;
- causa cancellazione: `cancellation_code`;
- cause ritardo: `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay`.

## 3. Preparazione e pulizia dei dati

La Fase 2 produce un dataset processed comune, salvato in HDFS in formato Parquet e CSV, da usare come input unico per Spark SQL, Spark Core e Hive.

Input raw:

```text
/data/raw/flight_data_2024.csv
```

Output processed:

```text
/data/processed/flights_2024_clean.parquet
/data/processed/flights_2024_clean_csv
```

Il formato Parquet viene scelto come input principale delle analisi perche conserva lo schema, riduce lo spazio occupato e rende piu efficiente la lettura selettiva delle colonne. Il CSV viene mantenuto come output ispezionabile e compatibile con strumenti semplici.

### 3.1 Colonne mantenute

Le colonne mantenute coprono tutte le informazioni necessarie alle analisi 3.1, 3.2 e 3.3:

- identificazione temporale: `year`, `month`, `day_of_month`, `day_of_week`, `fl_date`;
- compagnia e volo: `op_unique_carrier`, `op_carrier_fl_num`;
- aeroporti e tratta: `origin`, `origin_city_name`, `origin_state_nm`, `dest`, `dest_city_name`, `dest_state_nm`;
- ritardi: `dep_delay`, `arr_delay`;
- cancellazioni: `cancelled`, `cancellation_code`;
- cause di ritardo: `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay`;
- variabile di controllo: `distance`.

Sono state aggiunte le colonne derivate:

- `route`, ottenuta concatenando `origin` e `dest`;
- `is_cancelled`, flag normalizzato per il calcolo del tasso di cancellazione;
- `primary_disruption_cause`, causa principale di ritardo o cancellazione;
- `departure_delay_band`, fascia di ritardo in partenza per l'analisi 3.2.

### 3.2 Colonne eliminate

Sono state eliminate solo colonne non richieste dalle analisi previste:

- `crs_dep_time` e `dep_time`, perche le analisi usano mese, aeroporto, compagnia e ritardo aggregato, non l'orario del giorno;
- `taxi_out`, `wheels_off`, `wheels_on`, `taxi_in`, perche descrivono fasi operative del volo non richieste;
- `crs_arr_time` e `arr_time`, perche le analisi usano `arr_delay`, non l'orario effettivo di arrivo;
- `crs_elapsed_time`, `actual_elapsed_time`, `air_time`, utili per analisi di durata ma non per statistiche compagnia, report ritardi o ranking anomalie.

Non viene eliminata nessuna colonna relativa a compagnia, aeroporti, mese, ritardi, cancellazioni o cause.

La colonna `diverted` viene usata durante la pulizia per rimuovere i voli deviati, ma non viene salvata nel dataset processed. Dopo il filtro avrebbe sempre valore 0 e quindi non aggiungerebbe informazione utile alle analisi successive.

### 3.3 Regole di normalizzazione e pulizia

La pipeline converte `fl_date` in tipo data, normalizza campi numerici e flag, e applica `trim`/uppercase ai codici categorici principali.

Sono rimossi i record:

- senza `fl_date`, `month`, `op_unique_carrier`, `origin` o `dest`;
- con `month` fuori dall'intervallo 1-12;
- con `origin = dest`;
- con `diverted = 1`.

I voli cancellati vengono mantenuti, perche necessari al calcolo del tasso di cancellazione. Per questi record, eventuali valori mancanti in `dep_delay` e `arr_delay` restano nulli e saranno esclusi automaticamente dalle medie Spark/Hive.

La colonna `primary_disruption_cause` viene derivata usando `cancellation_code` per i voli cancellati e, per i voli non cancellati, scegliendo la causa con valore massimo tra `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay` e `late_aircraft_delay`. Se non esiste una causa valorizzata, viene assegnato `none`.

La colonna `departure_delay_band` usa le fasce richieste dalla traccia:

- `low`: ritardo in partenza minore di 15 minuti;
- `medium`: ritardo tra 15 e 60 minuti inclusi;
- `high`: ritardo maggiore di 60 minuti;
- `unknown`: ritardo in partenza non disponibile.

### 3.4 Stato output

La pipeline e implementata in `src/prepare_clean_dataset.py` ed eseguita tramite `scripts/prepare_clean_dataset.ps1`.

Conteggi prodotti dalla pipeline:

- righe raw: 7.079.081;
- righe processed: 7.061.582;
- righe rimosse: 17.499;
- righe con chiavi nulle nel processed: 0;
- righe deviate dopo il filtro: 0.

## 4. Analisi 3.1 - Statistiche delle compagnie aeree

Da completare nelle Fasi 3, 5 e 6.

Metriche previste:

- compagnia;
- tratta o aeroporto servito;
- numero voli;
- ritardo minimo in arrivo;
- ritardo massimo in arrivo;
- ritardo medio in arrivo;
- tasso cancellazione;
- mesi operativi.

### Spark SQL

Da completare.

### Spark Core

Da completare.

### Hive

Da completare.

## 5. Analisi 3.2 - Report ritardi per aeroporto e mese

Da completare nelle Fasi 4, 5 e 6.

Metriche previste:

- aeroporto di partenza;
- mese;
- fascia ritardo in partenza: basso, medio, alto;
- numero voli per fascia;
- ritardo medio in partenza per fascia;
- ritardo medio in arrivo per fascia;
- top 3 cause disponibili.

### Spark SQL

Da completare.

### Spark Core

Da completare.

### Hive

Da completare.

## 6. Analisi 3.3 - Ranking anomalie compagnia-aeroporto

Estensione opzionale.

Da completare se il tempo lo permette.

## 7. Benchmark

Da completare nella Fase 7.

Dimensioni previste:

- 100k righe;
- 500k righe;
- 1M righe;
- dataset completo.

## 8. Grafici

I grafici saranno salvati in `reports/figures`.

Da completare nella Fase 7.

## 9. Confronto critico

Da completare nella Fase 8.

Aspetti da discutere:

- espressivita delle tecnologie;
- semplicita implementativa;
- efficienza;
- scalabilita;
- impatto di shuffle, aggregazioni e preparazione dati.

## 10. Riproducibilita

La Fase 1 introduce un ambiente locale basato su Docker Compose con:

- HDFS, composto da NameNode e DataNode;
- Hive, composto da metastore PostgreSQL, Hive Metastore e HiveServer2;
- Spark, composto da Spark Master e Spark Worker.

Il dataset originale viene mantenuto fuori dal repository Git e caricato in HDFS tramite lo script `scripts/load_dataset_to_hdfs.ps1`.

Stato Fase 1: completata. Il file `flight_data_2024.csv` e stato caricato in HDFS nel percorso `/data/raw/flight_data_2024.csv`.

Comandi principali:

```powershell
.\scripts\start.ps1
.\scripts\status.ps1
.\scripts\check_environment.ps1
.\scripts\load_dataset_to_hdfs.ps1
.\scripts\stop.ps1
```

Elementi previsti:

- prerequisiti: Docker Desktop con Docker Compose;
- setup Docker tramite `docker-compose.yml`;
- caricamento dataset in HDFS;
- esecuzione analisi;
- generazione benchmark;
- generazione report PDF.

## 11. Repository GitHub

Da aggiornare con il link al repository finale.
