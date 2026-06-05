# Esecuzione benchmark su AWS Academy con EMR

Questa procedura permette di eseguire su Amazon EMR gli stessi benchmark locali del progetto, mantenendo separate le uscite AWS da quelle HDFS/Docker locali.

## 1. Scelta ambiente

La prima scelta e Amazon EMR perche include Spark, Hive, Hadoop/YARN e integrazione S3. Nel Learner Lab e stata usata questa configurazione:

- applicazioni: Hadoop, Spark, Hive;
- 1 nodo master `m5.xlarge`;
- 2 nodi core `m5.2xlarge`;
- 32-64 GB EBS per nodo;
- log cluster su S3;
- terminazione cluster appena finiti i benchmark.

Dipendenze necessarie:

- AWS CLI configurata nel Learner Lab o sul terminale locale;
- Bash per eseguire gli script `scripts/aws/*.sh`;
- Python 3 per consolidare i CSV AWS;
- su EMR: Spark, Hive, Hadoop/YARN e permessi di lettura/scrittura sul bucket S3.

## 2. Layout S3

Impostare un bucket personale e un prefisso di progetto:

```bash
export S3_BUCKET="nome-bucket"
export S3_PREFIX="flight-delay-project"
```

Creare il bucket, se non esiste gia:

```bash
aws s3 mb "s3://${S3_BUCKET}"
```

Gli script usano questo layout:

```text
s3://<bucket>/<prefix>/raw/
s3://<bucket>/<prefix>/processed/
s3://<bucket>/<prefix>/samples/
s3://<bucket>/<prefix>/scaled/
s3://<bucket>/<prefix>/outputs/aws/
s3://<bucket>/<prefix>/benchmarks/aws/
s3://<bucket>/<prefix>/logs/
```

## 3. Upload locale verso S3

Da Git Bash, WSL o CloudShell, con AWS CLI configurata:

```bash
export S3_BUCKET="nome-bucket"
export S3_PREFIX="flight-delay-project"
./scripts/aws/upload_project_to_s3.sh data/raw/flight_data_2024.csv
```

Lo script carica `src/`, `scripts/aws/` e, se presente, il CSV raw.

## 4. Preparazione nodo master EMR

Creare il cluster EMR dalla console AWS Academy scegliendo:

- release EMR recente disponibile nel Learner Lab;
- applicazioni Hadoop, Spark e Hive;
- 1 nodo master `m5.xlarge`;
- 2 nodi core `m5.2xlarge`;
- log su `s3://<bucket>/<prefix>/logs/emr/`.

Collegarsi via SSH al nodo master EMR e scaricare codice e script:

```bash
export S3_BUCKET="nome-bucket"
export S3_PREFIX="flight-delay-project"
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/src" ./src
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/scripts/aws" ./scripts/aws
chmod +x scripts/aws/*.sh
```

## 5. Preparazione dataset

La preparazione non entra nei tempi di benchmark:

```bash
./scripts/aws/prepare_clean_dataset.sh
./scripts/aws/prepare_benchmark_samples.sh
./scripts/aws/prepare_scalability_datasets.sh
```

Output attesi:

```text
s3://<bucket>/<prefix>/processed/flights_2024_clean.parquet
s3://<bucket>/<prefix>/samples/flights_clean_100k.parquet
s3://<bucket>/<prefix>/samples/flights_clean_500k.parquet
s3://<bucket>/<prefix>/samples/flights_clean_half.parquet
s3://<bucket>/<prefix>/samples/flights_clean_full.parquet
s3://<bucket>/<prefix>/scaled/flights_clean_1x.parquet
s3://<bucket>/<prefix>/scaled/flights_clean_2x.parquet
s3://<bucket>/<prefix>/scaled/flights_clean_4x.parquet
```

## 6. Esecuzione benchmark

Eseguire prima un test piccolo:

```bash
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_sql 100k
./scripts/aws/run_spark_analysis.sh analysis_3_1 spark_core 100k
./scripts/aws/run_hive_analysis.sh analysis_3_1 100k
```

Poi eseguire tutti i benchmark sample:

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

In alternativa, dopo aver verificato il test `100k`:

```bash
./scripts/aws/run_all_benchmarks.sh
```

## 7. Raccolta risultati

I CSV tempi vengono salvati in S3:

```text
s3://<bucket>/<prefix>/benchmarks/aws/<analysis>/<technology>/timings.csv
s3://<bucket>/<prefix>/benchmarks/aws/scalability/<analysis>/<technology>/timings.csv
```

Gli output completi e i log vengono salvati in:

```text
s3://<bucket>/<prefix>/outputs/aws/
s3://<bucket>/<prefix>/logs/
```

Per scaricare i timing nel repository locale:

```bash
export S3_BUCKET="nome-bucket"
export S3_PREFIX="flight-delay-project"
./scripts/aws/download_aws_benchmarks.sh
./scripts/aws/consolidate_aws_benchmarks.sh
```

Da PowerShell locale:

```powershell
$aws = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
$bucket = "flight-delay-5138901"
$prefix = "flight-delay-project"

& $aws s3 sync "s3://$bucket/$prefix/benchmarks/aws" .\outputs\benchmarks\aws
python .\src\generate_aws_benchmark_summary.py
python .\src\generate_benchmark_figures.py
```

CSV consolidati locali:

```text
outputs/benchmarks/aws_benchmark_summary.csv
outputs/benchmarks/aws_scalability_summary.csv
```

Grafici AWS locali:

```text
reports/figures/aws_benchmark_analysis_3_1.svg
reports/figures/aws_benchmark_analysis_3_2.svg
reports/figures/aws_scalability_analysis_3_1.svg
reports/figures/aws_scalability_analysis_3_2.svg
reports/figures/aws_combined_analysis_3_1.svg
reports/figures/aws_combined_analysis_3_2.svg
```

## 8. Aggiornamento report

Dopo il download dei timing AWS, aggiungere al report:

- configurazione EMR usata;
- tempi locali e tempi AWS affiancati;
- commento su differenze dovute a cluster, YARN, S3 e overhead di avvio job;
- nota sui limiti del Learner Lab e sui run eventualmente non completati.
