# Flight Delay Project

Progetto Big Data sul dataset Flight Delay Dataset 2024. L'obiettivo è confrontare diverse tecnologie per analisi su dati di grandi dimensioni, con attenzione a preparazione dati, implementazione, tempi di esecuzione e scalabilità.

## Stato del progetto

Stato: **progetto completato**.

Roadmap completa: [docs/roadmap.md](docs/roadmap.md)

Report finale: [reports/report.md](reports/report.md)

## Tecnologie previste

Tecnologie obbligatorie:

- Spark SQL
- Spark Core
- Hive

## Analisi previste

Analisi minime:

- 3.1 Statistiche delle compagnie aeree
- 3.2 Report dei ritardi per aeroporto e periodo temporale

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
  aws_emr.md    procedura AWS Academy con EMR
scripts/        script di setup, esecuzione e benchmark
  aws/          script bash per esecuzione su Amazon EMR
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
/data/samples/flights_clean_<run_size>.parquet
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
/data/samples/flights_clean_<run_size>.parquet
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

### Preparazione sample benchmark

Prima di eseguire benchmark confrontabili, preparare i sample deterministici comuni:

```powershell
.\scripts\prepare_benchmark_samples.ps1
```

Lo script legge il dataset pulito:

```text
/data/processed/flights_2024_clean.parquet
```

e crea i Parquet usati da tutte le tecnologie:

```text
/data/samples/flights_clean_100k.parquet
/data/samples/flights_clean_500k.parquet
/data/samples/flights_clean_half.parquet
/data/samples/flights_clean_full.parquet
```

I run `100k`, `500k` e `half` non applicano piu `LIMIT` dentro le singole tecnologie: Spark SQL, Spark Core e Hive leggono direttamente lo stesso sample gia materializzato.

### Test di efficienza e scalabilita

Per testare la scalabilita su volumi crescenti, creare dataset Parquet materializzati duplicando il dataset processed:

```powershell
.\scripts\prepare_scalability_datasets.ps1
```

Output HDFS:

```text
/data/scaled/flights_clean_1x.parquet
/data/scaled/flights_clean_2x.parquet
/data/scaled/flights_clean_4x.parquet
```

I fattori `1x`, `2x` e `4x` mantengono schema e distribuzione del dataset originale, ma aumentano il numero di righe per osservare la crescita dei tempi. La preparazione dei dataset scalati e separata dai benchmark: le analisi leggono dataset gia salvati in HDFS.

Eseguire i benchmark di scalabilita:

```powershell
.\scripts\run_analysis_3_1_spark_sql.ps1 -RunSize scale_all
.\scripts\run_analysis_3_1_spark_core.ps1 -RunSize scale_all
.\scripts\run_analysis_3_1_hive.ps1 -RunSize scale_all
.\scripts\run_analysis_3_2_spark_sql.ps1 -RunSize scale_all
.\scripts\run_analysis_3_2_spark_core.ps1 -RunSize scale_all
.\scripts\run_analysis_3_2_hive.ps1 -RunSize scale_all
```

Output HDFS separati dai benchmark sui sample:

```text
/outputs/scalability/analysis_3_1/<technology>/<scale>/
/outputs/scalability/analysis_3_2/<technology>/<scale>/
/outputs/benchmarks/scalability/analysis_3_1/<technology>/timings.csv
/outputs/benchmarks/scalability/analysis_3_2/<technology>/timings.csv
```

### Benchmark e grafici

La Fase 7 consolida i timing presenti in `outputs/benchmarks` e genera i grafici per il report. I benchmark sui sample (`100k`, `500k`, `half`, `full`) restano separati dai benchmark di scalabilita (`1x`, `2x`, `4x`).

Generare CSV consolidato e figure SVG:

```powershell
.\scripts\generate_benchmark_figures.ps1
```

Output locali:

```text
outputs/benchmarks/benchmark_summary.csv
outputs/benchmarks/scalability_summary.csv
reports/figures/benchmark_analysis_3_1.svg
reports/figures/benchmark_analysis_3_2.svg
reports/figures/scalability_analysis_3_1.svg
reports/figures/scalability_analysis_3_2.svg
reports/figures/combined_analysis_3_1.svg
reports/figures/combined_analysis_3_2.svg
```

I grafici benchmark usano `100k`, `500k`, `half` e `full`; i grafici di scalabilita usano `1x`, `2x` e `4x`; i grafici combinati mostrano entrambe le sequenze nello stesso asse X.

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

## Esecuzione su AWS Academy con EMR

La procedura cloud usa Amazon EMR come prima scelta, perche integra Spark, Hive, Hadoop/YARN e accesso a S3. Le istruzioni complete sono in [docs/aws_emr.md](docs/aws_emr.md).

Dipendenze: AWS CLI, Bash, Python 3 e un cluster EMR con Hadoop, Spark e Hive abilitati.

### Configurazione consigliata

Per ridurre il consumo del credito AWS Academy:

- cluster EMR con Hadoop, Spark e Hive;
- 1 nodo master e 1 nodo core;
- istanze `m5.xlarge` se disponibili, oppure `m5.large` per una prova piu economica;
- EBS 32-64 GB per nodo;
- terminare il cluster appena completati i benchmark.

### Upload su S3

Impostare bucket e prefisso:

```bash
export S3_BUCKET="nome-bucket"
export S3_PREFIX="flight-delay-project"
```

Caricare codice, script AWS e dataset raw:

```bash
aws s3 mb "s3://${S3_BUCKET}"
./scripts/aws/upload_project_to_s3.sh data/raw/flight_data_2024.csv
```

Layout S3 usato dagli script:

```text
s3://<bucket>/<prefix>/raw/
s3://<bucket>/<prefix>/processed/
s3://<bucket>/<prefix>/samples/
s3://<bucket>/<prefix>/scaled/
s3://<bucket>/<prefix>/outputs/aws/
s3://<bucket>/<prefix>/benchmarks/aws/
s3://<bucket>/<prefix>/logs/
```

### Preparazione sul nodo master EMR

Dopo il collegamento SSH al nodo master:

```bash
export S3_BUCKET="nome-bucket"
export S3_PREFIX="flight-delay-project"
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/src" ./src
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/scripts/aws" ./scripts/aws
chmod +x scripts/aws/*.sh
```

Preparare dataset clean, sample e dataset scalati. Questi tempi non fanno parte dei benchmark:

```bash
./scripts/aws/prepare_clean_dataset.sh
./scripts/aws/prepare_benchmark_samples.sh
./scripts/aws/prepare_scalability_datasets.sh
```

### Benchmark AWS

Eseguire prima un test rapido:

```bash
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_sql 100k
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_core 100k
./scripts/aws/run_hive_analysis.sh analysis_3_1 100k
```

Eseguire tutti i benchmark sample:

```bash
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_sql all
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_core all
./scripts/aws/run_hive_analysis.sh analysis_3_1 all
./scripts/aws/run_spark_analysis.sh analysis_3_2 spark_sql all
./scripts/aws/run_spark_analysis.sh analysis_3_2 spark_core all
./scripts/aws/run_hive_analysis.sh analysis_3_2 all
```

Eseguire i benchmark di scalabilita:

```bash
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_sql scale_all
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_core scale_all
./scripts/aws/run_hive_analysis.sh analysis_3_1 scale_all
./scripts/aws/run_spark_analysis.sh analysis_3_2 spark_sql scale_all
./scripts/aws/run_spark_analysis.sh analysis_3_2 spark_core scale_all
./scripts/aws/run_hive_analysis.sh analysis_3_2 scale_all
```

I timing AWS vengono salvati in CSV separati:

```text
s3://<bucket>/<prefix>/benchmarks/aws/<analysis>/<technology>/timings.csv
s3://<bucket>/<prefix>/benchmarks/aws/scalability/<analysis>/<technology>/timings.csv
```

Scaricarli localmente:

```bash
./scripts/aws/download_aws_benchmarks.sh
./scripts/aws/consolidate_aws_benchmarks.sh
```

Da PowerShell locale, in alternativa:

```powershell
$aws = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
$bucket = "flight-delay-5138901"
$prefix = "flight-delay-project"

& $aws s3 sync "s3://$bucket/$prefix/benchmarks/aws" .\outputs\benchmarks\aws
python .\src\generate_aws_benchmark_summary.py
python .\src\generate_benchmark_figures.py
```

Il confronto finale nel report deve affiancare i tempi locali e AWS, specificando configurazione EMR, dimensione input, tecnologia, analisi, tempo di esecuzione e righe output.

CSV consolidati AWS locali:

```text
outputs/benchmarks/aws_benchmark_summary.csv
outputs/benchmarks/aws_scalability_summary.csv
```

Grafici AWS generati localmente:

```text
reports/figures/aws_benchmark_analysis_3_1.svg
reports/figures/aws_benchmark_analysis_3_2.svg
reports/figures/aws_scalability_analysis_3_1.svg
reports/figures/aws_scalability_analysis_3_2.svg
reports/figures/aws_combined_analysis_3_1.svg
reports/figures/aws_combined_analysis_3_2.svg
```
