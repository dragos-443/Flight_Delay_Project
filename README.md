# Flight Delay Project

Progetto Big Data sul dataset Flight Delay Dataset 2024. L'obiettivo e confrontare diverse tecnologie per analisi su dati di grandi dimensioni, con attenzione a preparazione dati, implementazione, tempi di esecuzione e scalabilita.

## Stato del progetto

Fase corrente: **Fase 1 completata**. Prossima fase: **Fase 2 - Preparazione e pulizia dati**.

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

Gli script applicativi saranno aggiunti nelle fasi successive:

- Fase 2: preparazione e pulizia dati
- Fasi 3-6: job Spark SQL, Spark Core e Hive
- Fase 7: benchmark e grafici

## Esecuzione futura su AWS

La struttura del progetto evita path assoluti e separa configurazioni, dati e codice per facilitare una futura esecuzione su cluster Spark/Hadoop in ambiente cloud. Le istruzioni AWS saranno aggiunte quando verra definito l'ambiente di esecuzione.

Per ora viene mantenuta una configurazione locale semplice. Quando si passera ad AWS, verra aggiunta una configurazione separata invece di forzare tutte le differenze dentro il `.env` locale.
