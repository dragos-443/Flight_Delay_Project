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

Da completare nella Fase 2.

Aspetti da documentare:

- selezione colonne;
- normalizzazione tipi;
- gestione valori nulli;
- rimozione record non significativi;
- derivazione tratta;
- derivazione causa principale di ritardo o cancellazione.

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
