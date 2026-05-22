# Flight Delay Project

Progetto Big Data sul dataset Flight Delay Dataset 2024. L'obiettivo e confrontare diverse tecnologie per analisi su dati di grandi dimensioni, con attenzione a preparazione dati, implementazione, tempi di esecuzione e scalabilita.

## Stato del progetto

Fase corrente: **Fase 0 completata**. Prossima fase: **Fase 1 - Ambiente Docker locale**.

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

## Dataset

Il dataset completo non viene incluso nel repository Git. Il file locale atteso e:

```text
data/raw/flight_data_2024.csv
```

In questa fase il CSV puo anche essere presente nella root del progetto, ma resta ignorato da Git. Nelle fasi successive verra caricato in HDFS tramite Docker.

## Esecuzione locale

Gli script di esecuzione saranno aggiunti nelle fasi successive:

- Fase 1: ambiente Docker locale con HDFS, Hive e Spark
- Fase 2: preparazione e pulizia dati
- Fasi 3-6: job Spark SQL, Spark Core e Hive
- Fase 7: benchmark e grafici

## Esecuzione futura su AWS

La struttura del progetto evita path assoluti e separa configurazioni, dati e codice per facilitare una futura esecuzione su cluster Spark/Hadoop in ambiente cloud. Le istruzioni AWS saranno aggiunte quando verra definito l'ambiente di esecuzione.
