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

La prima implementazione dell'analisi 3.1 e stata realizzata con Spark SQL nella Fase 3. La replica Spark Core e stata completata nella Fase 5; la replica Hive sara completata nella fase successiva.

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

Implementazione: `src/analysis_3_1_spark_sql.py`.

Script di esecuzione: `scripts/run_analysis_3_1_spark_sql.ps1`.

Input:

```text
/data/processed/flights_2024_clean.parquet
```

Output:

```text
/outputs/analysis_3_1/spark_sql/
  100k/
    csv/
    parquet/
  500k/
    csv/
    parquet/
  half/
    csv/
    parquet/
  full/
    csv/
    parquet/
```

Gli output completi sono salvati in HDFS, mentre nel repository vengono mantenuti codice, script, report e benchmark. Questa scelta evita di versionare file generati e potenzialmente grandi; se necessario, i CSV completi possono essere esportati localmente da HDFS con `hdfs dfs -getmerge`.

Tempi preliminari:

```text
/outputs/benchmarks/analysis_3_1/spark_sql/timings.csv
```

La query aggrega i dati per compagnia (`op_unique_carrier`) e tratta (`route`). Per ogni gruppo calcola numero voli, ritardo minimo, massimo e medio in arrivo, tasso di cancellazione e mesi operativi. Il tasso di cancellazione e salvato come frazione tra 0 e 1: ad esempio `0.0138` corrisponde a circa `1.38%`.

Le prove progressive sono state eseguite per validare il job su volumi crescenti. I tempi sono preliminari e saranno riusati nella Fase 7 per il confronto con Spark Core e Hive.

| Run | Tempo esecuzione (s) | Righe output |
| --- | ---: | ---: |
| 100k | 28.914 | 9.791 |
| 500k | 27.878 | 9.868 |
| half | 38.460 | 12.291 |
| full | 89.990 | 13.248 |

Prime 10 righe dell'output `full`:

| op_unique_carrier | route | flight_count | min_arr_delay | max_arr_delay | avg_arr_delay | cancellation_rate | operating_months |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 9E | ABE-ATL | 1015 | -47.0 | 575.0 | 0.67 | 0.0138 | 1,2,3,4,5,6,7,8,9,10,11,12 |
| 9E | ABY-ATL | 38 | -32.0 | 321.0 | 4.37 | 0.0 | 1,2,3,11,12 |
| 9E | AEX-ATL | 877 | -34.0 | 979.0 | 9.61 | 0.0103 | 1,2,3,4,5,6,7,8,9,10,11,12 |
| 9E | AGS-ATL | 1579 | -36.0 | 1091.0 | 6.89 | 0.0209 | 1,2,3,4,5,6,7,8,9,10,11,12 |
| 9E | AGS-AUS | 2 | 12.0 | 92.0 | 52.0 | 0.0 | 4 |
| 9E | AGS-DCA | 2 | 21.0 | 146.0 | 83.5 | 0.0 | 4 |
| 9E | AGS-DTW | 6 | -15.0 | 9.0 | -3.17 | 0.0 | 4 |
| 9E | AGS-JFK | 2 | -3.0 | 44.0 | 20.5 | 0.0 | 4 |
| 9E | AGS-LGA | 11 | -17.0 | 43.0 | -1.0 | 0.0 | 4 |
| 9E | ALB-DTW | 435 | -51.0 | 394.0 | -4.31 | 0.0046 | 1,2,3,4,5,6,7,8,9,10,11,12 |

### Spark Core

Implementazione: `src/analysis_3_1_spark_core.py`.

Script di esecuzione: `scripts/run_analysis_3_1_spark_core.ps1`.

Output:

```text
/outputs/analysis_3_1/spark_core/
  100k/
    csv/
    parquet/
  500k/
    csv/
    parquet/
  half/
    csv/
    parquet/
  full/
    csv/
    parquet/
```

Tempi preliminari:

```text
/outputs/benchmarks/analysis_3_1/spark_core/timings.csv
```

La replica Spark Core legge lo stesso Parquet pulito usato da Spark SQL, converte i record in RDD e calcola le metriche con trasformazioni `map`, `reduceByKey` e `sortBy`. Il risultato viene poi riconvertito in DataFrame solo per salvare gli output in CSV e Parquet con lo stesso schema della versione SQL.

| Run | Tempo esecuzione (s) | Righe output |
| --- | ---: | ---: |
| 100k | 27.343 | 9.791 |
| 500k | 40.116 | 9.868 |
| half | 131.851 | 12.291 |
| full | 139.875 | 13.248 |

Il confronto automatico tra output Parquet Spark Core e Spark SQL ha prodotto `left_only_rows=0` e `right_only_rows=0` sia sul sample `100k` sia sul dataset `full`.

Le prime 10 righe dell'output `full` coincidono con quelle riportate per Spark SQL.

### Hive

Da completare.

## 5. Analisi 3.2 - Report ritardi per aeroporto e mese

La prima implementazione dell'analisi 3.2 e stata realizzata con Spark SQL nella Fase 4. La replica Spark Core e stata completata nella Fase 5; la replica Hive sara completata nella fase successiva.

Metriche previste:

- aeroporto di partenza;
- mese;
- fascia ritardo in partenza: basso, medio, alto;
- numero voli per fascia;
- ritardo medio in partenza per fascia;
- ritardo medio in arrivo per fascia;
- top 3 cause disponibili.

### Spark SQL

Implementazione: `src/analysis_3_2_spark_sql.py`.

Script di esecuzione: `scripts/run_analysis_3_2_spark_sql.ps1`.

Input:

```text
/data/processed/flights_2024_clean.parquet
```

Output:

```text
/outputs/analysis_3_2/spark_sql/
  100k/
    csv/
    parquet/
  500k/
    csv/
    parquet/
  half/
    csv/
    parquet/
  full/
    csv/
    parquet/
```

Gli output completi sono salvati in HDFS, mentre nel repository vengono mantenuti codice, script, report e benchmark. Questa scelta evita di versionare file generati e potenzialmente grandi; se necessario, i CSV completi possono essere esportati localmente da HDFS con `hdfs dfs -getmerge`.

Tempi preliminari:

```text
/outputs/benchmarks/analysis_3_2/spark_sql/timings.csv
```

La query considera le tre fasce richieste dalla traccia (`low`, `medium`, `high`) ed esclude la fascia `unknown`, che contiene voli senza ritardo in partenza disponibile. Il report produce una riga per aeroporto di partenza, mese e fascia. Per ogni gruppo calcola numero voli, ritardo medio in partenza, ritardo medio in arrivo e le tre cause piu frequenti tra quelle disponibili. Le cause sono riportate nel formato `causa:conteggio`; quando non sono disponibili cause valorizzate viene scritto `none`.

Le prove progressive sono state eseguite per validare il job su volumi crescenti. I tempi sono preliminari e saranno riusati nella Fase 7 per il confronto con Spark Core e Hive.

| Run | Tempo esecuzione (s) | Righe output |
| --- | ---: | ---: |
| 100k | 52.879 | 920 |
| 500k | 49.841 | 1.817 |
| half | 93.491 | 7.780 |
| full | 126.203 | 11.902 |

Prime 10 righe dell'output `full`:

| origin | month | departure_delay_band | flight_count | avg_dep_delay | avg_arr_delay | top_3_causes |
| --- | ---: | --- | ---: | ---: | ---: | --- |
| ABE | 1 | low | 275 | -5.51 | -14.56 | nas:20,carrier:1 |
| ABE | 1 | medium | 30 | 34.9 | 32.5 | late_aircraft:12,carrier:8,nas:3 |
| ABE | 1 | high | 30 | 241.3 | 234.03 | late_aircraft:14,carrier:12,nas:2 |
| ABE | 2 | low | 296 | -6.62 | -19.57 | nas:10,carrier:1,weather:1 |
| ABE | 2 | medium | 19 | 28.53 | 10.11 | carrier:5,late_aircraft:3 |
| ABE | 2 | high | 14 | 227.93 | 217.93 | carrier:6,late_aircraft:6,nas:1 |
| ABE | 3 | low | 339 | -6.19 | -18.37 | nas:7 |
| ABE | 3 | medium | 28 | 30.68 | 19.89 | late_aircraft:7,carrier:4,nas:4 |
| ABE | 3 | high | 23 | 173.43 | 163.39 | carrier:9,late_aircraft:8,nas:6 |
| ABE | 4 | low | 303 | -6.45 | -17.3 | nas:5,late_aircraft:1 |

### Spark Core

Implementazione: `src/analysis_3_2_spark_core.py`.

Script di esecuzione: `scripts/run_analysis_3_2_spark_core.ps1`.

Output:

```text
/outputs/analysis_3_2/spark_core/
  100k/
    csv/
    parquet/
  500k/
    csv/
    parquet/
  half/
    csv/
    parquet/
  full/
    csv/
    parquet/
```

Tempi preliminari:

```text
/outputs/benchmarks/analysis_3_2/spark_core/timings.csv
```

La replica Spark Core filtra le tre fasce richieste, aggrega i conteggi e le somme dei ritardi con RDD keyed per `(origin, month, departure_delay_band)` e calcola le top 3 cause con un secondo conteggio per causa. Anche in questo caso il DataFrame finale viene usato solo per materializzare output CSV e Parquet.

| Run | Tempo esecuzione (s) | Righe output |
| --- | ---: | ---: |
| 100k | 30.001 | 920 |
| 500k | 44.418 | 1.817 |
| half | 162.215 | 7.780 |
| full | 232.650 | 11.902 |

Il confronto automatico tra output Parquet Spark Core e Spark SQL ha prodotto `left_only_rows=0` e `right_only_rows=0` sia sul sample `100k` sia sul dataset `full`.

Le prime 10 righe dell'output `full` coincidono con quelle riportate per Spark SQL.

### Primo confronto Spark SQL e Spark Core

Spark SQL risulta piu compatto ed espressivo: le aggregazioni, l'ordinamento delle cause e le funzioni finestra sono descritti direttamente nella query. Spark Core richiede invece una gestione esplicita dello stato aggregato, delle chiavi composte, delle cause e dell'arrotondamento, ma rende piu visibile il flusso distribuito `map`/`reduceByKey`.

Nei test locali Docker, Spark Core produce output identici a Spark SQL ma con tempi generalmente piu alti sui run grandi, soprattutto per l'analisi 3.2. La differenza e coerente con la maggiore quantita di logica espressa lato Python/RDD e con la minore ottimizzazione rispetto al piano Spark SQL.

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
