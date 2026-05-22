# Roadmap Incrementale Flight Delay Project

Questa roadmap guida lo sviluppo del progetto per fasi. Ogni fase deve produrre un risultato verificabile e aggiornare README e report quando necessario.

## Fase 0 - Impostazione repository e documentazione base

Obiettivo: creare la base ordinata del progetto.

Task:

- inizializzare repository Git;
- creare `.gitignore`;
- creare `.env.example`;
- creare struttura cartelle;
- creare `README.md`;
- creare `docs/roadmap.md`;
- creare `reports/report.md`.

Criterio di completamento:

- struttura chiara;
- dataset grande escluso da Git;
- `.env` ignorato;
- README, roadmap e report presenti.

Stato: **completata**.

## Fase 1 - Ambiente Docker locale

Obiettivo: predisporre l'ambiente locale containerizzato.

Task:

- creare `docker-compose.yml`;
- configurare HDFS locale;
- aggiungere Hive;
- aggiungere Spark;
- creare script per avvio e stop ambiente;
- creare script per caricare il CSV in HDFS;
- documentare tutto nel README.

Criterio di completamento:

- `docker compose up` avvia l'ambiente;
- HDFS e raggiungibile;
- Hive e Spark sono disponibili;
- il dataset puo essere caricato in HDFS.

Stato: **da fare**.

## Fase 2 - Preparazione e pulizia dati

Obiettivo: produrre un dataset processed comune per tutte le tecnologie.

Task:

- selezionare colonne rilevanti;
- normalizzare tipi numerici e categorici;
- rimuovere record non significativi;
- derivare `route = origin-dest`;
- derivare la causa principale di ritardo o cancellazione;
- salvare output in CSV e Parquet;
- aggiornare `reports/report.md` con motivazioni delle scelte.

Criterio di completamento:

- esiste un dataset pulito;
- la pulizia e documentata;
- tutte le tecnologie successive usano lo stesso input.

Stato: **da fare**.

## Fase 3 - Analisi 3.1 con Spark SQL

Obiettivo: implementare la prima analisi nella tecnologia piu semplice da validare.

Task:

- calcolare statistiche per compagnia e tratta;
- produrre numero voli;
- produrre min, max e media del ritardo in arrivo;
- produrre tasso cancellazione;
- produrre mesi operativi;
- salvare output CSV e Parquet;
- inserire prime 10 righe nel report.

Criterio di completamento:

- output corretto per analisi 3.1 con Spark SQL;
- risultati ispezionabili;
- report aggiornato.

Stato: **da fare**.

## Fase 4 - Analisi 3.2 con Spark SQL

Obiettivo: completare le due analisi minime con una prima tecnologia.

Task:

- calcolare report per aeroporto e mese;
- classificare ritardi in basso, medio, alto;
- calcolare conteggi per fascia;
- calcolare media ritardo partenza e arrivo per fascia;
- calcolare top 3 cause;
- salvare output CSV e Parquet;
- inserire prime 10 righe nel report.

Criterio di completamento:

- Spark SQL copre 3.1 e 3.2;
- la logica delle analisi e validata prima di replicarla.

Stato: **da fare**.

## Fase 5 - Replica in Spark Core

Obiettivo: implementare le stesse analisi usando RDD.

Task:

- implementare 3.1 in Spark Core;
- implementare 3.2 in Spark Core;
- confrontare risultati con Spark SQL sui sample;
- aggiornare report con differenze implementative.

Criterio di completamento:

- Spark Core produce output coerenti con Spark SQL;
- il report contiene un primo confronto tecnico.

Stato: **da fare**.

## Fase 6 - Replica in Hive

Obiettivo: completare le tre tecnologie obbligatorie.

Task:

- creare tabelle esterne Hive;
- implementare query 3.1;
- implementare query 3.2;
- esportare risultati in CSV o tabelle output;
- confrontare risultati con Spark SQL;
- aggiornare report.

Criterio di completamento:

- Hive produce output per 3.1 e 3.2;
- requisiti minimi sulle tecnologie soddisfatti.

Stato: **da fare**.

## Fase 7 - Benchmark e grafici

Obiettivo: costruire la parte sperimentale.

Task:

- generare sample crescenti: 100k, 500k, 1M, full dataset;
- creare script di benchmark;
- misurare tempi per tecnologia, analisi e dimensione input;
- salvare risultati in `outputs/benchmarks`;
- generare grafici in `reports/figures`;
- aggiornare report con tabelle e grafici.

Criterio di completamento:

- esistono dati sperimentali confrontabili;
- il report contiene grafici e commento iniziale.

Stato: **da fare**.

## Fase 8 - Analisi critica e rifinitura report

Obiettivo: trasformare gli output tecnici in una relazione consegnabile.

Task:

- commentare espressivita delle tecnologie;
- commentare semplicita implementativa;
- commentare efficienza e scalabilita;
- discutere shuffle, aggregazioni e preparazione dati;
- aggiungere istruzioni complete di riproducibilita;
- esportare `report.md` in PDF.

Criterio di completamento:

- report finale completo;
- README completo;
- repository consegnabile.

Stato: **da fare**.

## Fase 9 - Estensione opzionale: Analisi 3.3

Obiettivo: aggiungere il ranking anomalie compagnia-aeroporto.

Task:

- implementare 3.3 in Spark SQL;
- replicare in Spark Core;
- replicare in Hive se il tempo lo permette;
- aggiungere output, benchmark e report.

Criterio di completamento:

- progetto copre tutte le analisi della traccia.

Stato: **opzionale**.

## Fase 10 - Estensione opzionale: MapReduce Python

Obiettivo: aggiungere la quarta tecnologia.

Task:

- predisporre Hadoop Streaming;
- implementare mapper e reducer Python per 3.1;
- implementare mapper e reducer Python per 3.2 se il tempo lo permette;
- confrontare risultati e tempi con le altre tecnologie;
- aggiornare report.

Criterio di completamento:

- progetto include anche MapReduce;
- confronto tecnologico piu completo.

Stato: **opzionale**.
