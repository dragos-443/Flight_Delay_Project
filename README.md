# Flight Delay Project

Progetto Big Data sul dataset Flight Delay Dataset 2024. L'obiettivo e confrontare diverse tecnologie per analisi su dati di grandi dimensioni, con attenzione a preparazione dati, implementazione, tempi di esecuzione e scalabilita.

## Stato del progetto

Fase corrente: **Fase 6 completata**. Prossima fase: **Fase 7 - Benchmark e grafici**.

Roadmap completa: [docs/roadmap.md](docs/roadmap.md)

Report in lavorazione: [reports/report.md](reports/report.md)

## Tecnologie previste

Tecnologie obbligatorie:

- Spark SQL
- Spark Core
- Hive

Estensione opzionale:

- MapReduce tramite Python e Hadoop Streaming

## Analisi previste

Analisi minime:

- 3.1 Statistiche delle compagnie aeree
- 3.2 Report dei ritardi per aeroporto e periodo temporale

Analisi opzionale:

- 3.3 Ranking delle coppie compagnia-aeroporto con comportamento anomalo nei ritardi

## Struttura del repository

```text
data/
  raw/          dataset originali locali, non versionati
  processed/    dataset puliti, non versionati
  samples/      campioni per benchmark, non versionati
outputs/        risultati dei job e benchmark, non versionati
reports/
  figures/      grafici generati
  report.md     relazione in sviluppo
docs/
  roadmap.md    roadmap incrementale
scripts/        script di setup, esecuzione e benchmark
src/            codice sorgente delle analisi
docker/         configurazioni Docker e servizi di supporto
```

## Configurazione locale

Creare un file `.env` a partire da `.env.example`:

```powershell
Copy-Item .env.example .env
```

Il file `.env` resta locale e non deve essere versionato.

Per sicurezza, le porte Docker vengono pubblicate solo su `127.0.0.1` tramite:

```env
DOCKER_BIND_ADDRESS=127.0.0.1
```

In un futuro ambiente remoto o cloud, questo valore potra essere cambiato a `0.0.0.0` solo se la macchina e protetta da firewall/security group adeguati.

## Dataset

Il dataset completo non viene incluso nel repository Git. Il file locale atteso e:

```text
data/raw/flight_data_2024.csv
```

In questa fase il CSV puo anche essere presente nella root del progetto, ma resta ignorato da Git. Nelle fasi successive verra caricato in HDFS tramite Docker.

## Esecuzione locale

### Avvio ambiente Docker

Avviare HDFS, Hive e Spark:

```powershell
.\scripts\start.ps1
```

Se PowerShell blocca gli script per execution policy:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

Controllare lo stato dei container:

```powershell
.\scripts\status.ps1
```

Verificare HDFS, Spark e Hive:

```powershell
.\scripts\check_environment.ps1
```

La stessa forma con `powershell -ExecutionPolicy Bypass -File ...` puo essere usata anche per gli altri script.

Interfacce web locali:

- HDFS NameNode: <http://localhost:9870>
- Spark Master: <http://localhost:18080>
- Spark Worker: <http://localhost:18081>

### Caricamento dataset in HDFS

Percorso consigliato del CSV:

```text
data/raw/flight_data_2024.csv
```

Caricare il dataset in HDFS:

```powershell
.\scripts\load_dataset_to_hdfs.ps1
```

Se il file si trova in un altro percorso:

```powershell
.\scripts\load_dataset_to_hdfs.ps1 -LocalPath "C:\percorso\flight_data_2024.csv"
```

Il file viene copiato in:

```text
/data/raw/flight_data_2024.csv
```

### Preparazione e pulizia dati

La Fase 2 produce un dataset processed comune per Spark SQL, Spark Core e Hive.

Eseguire la pipeline:

```powershell
.\scripts\prepare_clean_dataset.ps1
```

Se PowerShell blocca gli script per execution policy:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\prepare_clean_dataset.ps1
```

Input HDFS:

```text
/data/raw/flight_data_2024.csv
```

Output HDFS:

```text
/data/processed/flights_2024_clean.parquet
/data/processed/flights_2024_clean_csv
```

Il Parquet e l'input consigliato per le analisi successive. Il CSV processed viene prodotto per ispezione e compatibilita.

Verificare gli output:

```powershell
docker compose --env-file .env exec -T namenode hdfs dfs -ls -h /data/processed
```

### Analisi 3.1 con Spark SQL

La Fase 3 calcola le statistiche delle compagnie aeree per compagnia e tratta usando Spark SQL.

Input HDFS:

```text
/data/processed/flights_2024_clean.parquet
```

Eseguire una singola dimensione:

```powershell
.\scripts\run_analysis_3_1_spark_sql.ps1 -RunSize full
```

Eseguire tutte le prove progressive:

```powershell
.\scripts\run_analysis_3_1_spark_sql.ps1 -RunSize all
```

Valori supportati per `-RunSize`:

- `100k`
- `500k`
- `half`
- `full`
- `all`

Output HDFS:

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

Gli output completi delle analisi sono salvati in HDFS per non versionare file generati e potenzialmente grandi nel repository. Il report include solo le prime 10 righe richieste dalla traccia; i risultati completi possono essere ispezionati o esportati da HDFS quando necessario.

Tempi preliminari salvati per i benchmark futuri:

```text
/outputs/benchmarks/analysis_3_1/spark_sql/timings.csv
```

Verificare gli output:

```powershell
docker compose --env-file .env exec -T namenode hdfs dfs -ls -h /outputs/analysis_3_1/spark_sql
docker compose --env-file .env exec -T namenode hdfs dfs -cat /outputs/benchmarks/analysis_3_1/spark_sql/timings.csv
```

Esportare localmente il CSV completo del run `full`, solo se serve:

```powershell
docker compose --env-file .env exec -T namenode hdfs dfs -getmerge /outputs/analysis_3_1/spark_sql/full/csv /tmp/analysis_3_1_full.csv
docker compose --env-file .env cp namenode:/tmp/analysis_3_1_full.csv outputs/analysis_3_1_full.csv
```

### Analisi 3.1 con Spark Core

La Fase 5 replica l'analisi 3.1 usando trasformazioni RDD di Spark Core.

Eseguire una singola dimensione:

```powershell
.\scripts\run_analysis_3_1_spark_core.ps1 -RunSize full
```

Eseguire tutte le prove progressive:

```powershell
.\scripts\run_analysis_3_1_spark_core.ps1 -RunSize all
```

Confrontare l'output con Spark SQL per la stessa dimensione:

```powershell
.\scripts\run_analysis_3_1_spark_core.ps1 -RunSize 100k -CompareWithSparkSql
```

Output HDFS:

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

Il confronto tra tecnologie usa `src/compare_analysis_outputs.py`, uno script generico che riceve due output Parquet, li confronta con `exceptAll` in entrambe le direzioni e verifica che non esistano righe presenti solo in uno dei due risultati.

### Analisi 3.1 con Hive

La Fase 6 replica l'analisi 3.1 usando Hive su una tabella esterna Parquet.

Eseguire una singola dimensione:

```powershell
.\scripts\run_analysis_3_1_hive.ps1 -RunSize full
```

Eseguire tutte le prove progressive:

```powershell
.\scripts\run_analysis_3_1_hive.ps1 -RunSize all
```

Confrontare l'output `full` con Spark SQL:

```powershell
.\scripts\run_analysis_3_1_hive.ps1 -RunSize full -CompareWithSparkSql
```

Output HDFS:

```text
/outputs/analysis_3_1/hive/
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
/outputs/benchmarks/analysis_3_1/hive/timings.csv
```

Il flag `-CompareWithSparkSql` riusa lo stesso comparatore generico `src/compare_analysis_outputs.py`, passando come input l'output Parquet Hive e quello Spark SQL della stessa analisi.

### Analisi 3.2 con Spark SQL

La Fase 4 genera il report dei ritardi per aeroporto di partenza, mese e fascia di ritardo usando Spark SQL.

Input HDFS:

```text
/data/processed/flights_2024_clean.parquet
```

Eseguire una singola dimensione:

```powershell
.\scripts\run_analysis_3_2_spark_sql.ps1 -RunSize full
```

Eseguire tutte le prove progressive:

```powershell
.\scripts\run_analysis_3_2_spark_sql.ps1 -RunSize all
```

Valori supportati per `-RunSize`:

- `100k`
- `500k`
- `half`
- `full`
- `all`

Output HDFS:

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

Gli output completi delle analisi sono salvati in HDFS per non versionare file generati e potenzialmente grandi nel repository. Il report include solo le prime 10 righe richieste dalla traccia; i risultati completi possono essere ispezionati o esportati da HDFS quando necessario.

Tempi preliminari salvati per i benchmark futuri:

```text
/outputs/benchmarks/analysis_3_2/spark_sql/timings.csv
```

Verificare gli output:

```powershell
docker compose --env-file .env exec -T namenode hdfs dfs -ls -h /outputs/analysis_3_2/spark_sql
docker compose --env-file .env exec -T namenode hdfs dfs -cat /outputs/benchmarks/analysis_3_2/spark_sql/timings.csv
```

Esportare localmente il CSV completo del run `full`, solo se serve:

```powershell
docker compose --env-file .env exec -T namenode hdfs dfs -getmerge /outputs/analysis_3_2/spark_sql/full/csv /tmp/analysis_3_2_full.csv
docker compose --env-file .env cp namenode:/tmp/analysis_3_2_full.csv outputs/analysis_3_2_full.csv
```

### Analisi 3.2 con Spark Core

La Fase 5 replica l'analisi 3.2 usando trasformazioni RDD di Spark Core.

Eseguire una singola dimensione:

```powershell
.\scripts\run_analysis_3_2_spark_core.ps1 -RunSize full
```

Eseguire tutte le prove progressive:

```powershell
.\scripts\run_analysis_3_2_spark_core.ps1 -RunSize all
```

Confrontare l'output con Spark SQL per la stessa dimensione:

```powershell
.\scripts\run_analysis_3_2_spark_core.ps1 -RunSize 100k -CompareWithSparkSql
```

Output HDFS:

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

### Analisi 3.2 con Hive

La Fase 6 replica l'analisi 3.2 usando Hive su una tabella esterna Parquet.

Eseguire una singola dimensione:

```powershell
.\scripts\run_analysis_3_2_hive.ps1 -RunSize full
```

Eseguire tutte le prove progressive:

```powershell
.\scripts\run_analysis_3_2_hive.ps1 -RunSize all
```

Confrontare l'output `full` con Spark SQL:

```powershell
.\scripts\run_analysis_3_2_hive.ps1 -RunSize full -CompareWithSparkSql
```

Output HDFS:

```text
/outputs/analysis_3_2/hive/
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
/outputs/benchmarks/analysis_3_2/hive/timings.csv
```

Nota sui run limitati: Hive e Spark possono leggere i file Parquet in ordine fisico diverso quando si usa `LIMIT` senza ordinamento globale. Per questo i run `100k`, `500k` e `half` sono usati come benchmark progressivi della tecnologia, mentre il confronto esatto tra tecnologie viene eseguito sul run `full`, dove l'input e identico.

### Stop ambiente Docker

Fermare i container mantenendo i volumi:

```powershell
.\scripts\stop.ps1
```

Fermare i container eliminando anche i volumi Docker:

```powershell
.\scripts\stop.ps1 -Volumes
```

Usare `-Volumes` solo quando si vuole cancellare anche lo stato HDFS e il metastore Hive.

## Prossime fasi

- Fase 7: benchmark e grafici

## Esecuzione futura su AWS

La struttura del progetto evita path assoluti e separa configurazioni, dati e codice per facilitare una futura esecuzione su cluster Spark/Hadoop in ambiente cloud. Le istruzioni AWS saranno aggiunte quando verra definito l'ambiente di esecuzione.

Per ora viene mantenuta una configurazione locale semplice. Quando si passera ad AWS, verra aggiunta una configurazione separata invece di forzare tutte le differenze dentro il `.env` locale.
