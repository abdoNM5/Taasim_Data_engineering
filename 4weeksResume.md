# 4 Weeks Resume (Fr)

Ce document resume les taches et les roles des composants construits sur 4 semaines. Il ne detaille pas chaque semaine, mais explique le workflow global et la contribution de chaque piece.

## Objectif global
Construire un pipeline data temps-reel et batch pour simuler la mobilite urbaine de Casablanca, nettoyer les donnees, produire des evenements Kafka, et alimenter Cassandra/Grafana.

## Workflow global (vue rapide)
1) Ingestion et preparation des donnees de base (batch)
2) Geomapping Porto -> Casablanca et validation visuelle
3) Generation des trajectoires realistes (routes reelles)
4) Streaming temps-reel via Kafka
5) Traitement Flink (jobs 1, 2, 3)
6) Stockage Cassandra + visualisation Grafana

## Sources et preparation (batch)
- DataClean.ipynb: notebook principal pour la preparation des donnees. Il teleporte les trajectories de Porto vers Casablanca, applique un filtrage geographique, et produit un fichier CSV intermediaire.
- Sortie: casablanca_teleported.csv

## Geomapping et routes reelles
- Generate_Real_Routes.py: script qui prend casablanca_teleported.csv, telecharge le graphe OSM, calcule des routes reelles (pas d autoroutes), et produit un CSV final.
- Sortie: casablanca_real_roads_final.csv
- Validation visuelle: casablanca_map_100_trips.html (carte Folium de trajets echantillonnes)

## MinIO (data lake local)
- Buckets utilises: raw, curated, ml, kafka-archive
- Role: stockage des donnees brutes (train.csv), archivage et support pour les tests ETL

## Kafka (bus d evenements)
- Topics principaux:
  - raw.gps: flux GPS brut genere par ProducerGps.py
  - raw.trips: flux de demandes de trajets genere par ProducerTrips.py
  - processed.gps: sortie du Job 1 (GPS normalise)
  - processed.demand: sortie du Job 2 (ratio offre/demande)
  - processed.trips: sortie du Job 3 (matching taxi + trip)

## Producteurs (simulation temps-reel)
- ProducerGps.py: lit casablanca_real_roads_final.csv et simule un flux GPS taxi. Chaque polyline est eclatee en points, avec un leger bruit pour realisme, puis envoyee vers raw.gps.
  - Champs envoyes: taxi_id, timestamp (event_time), latitude, longitude, speed, status.
  - Effets realistes: bruit geographique + petits retards (blackout) sur l horodatage pour imiter des pannes reseau.
  - Usage: alimente Job 1 avec un flux continu de positions (1 point toutes ~50 ms par trajet).
- ProducerTrips.py: simule des demandes de trajets en temps-reel et les publie vers raw.trips.
  - Champs envoyes: trip_id, rider_id, origin_zone, destination_zone, requested_at, call_type.
  - Variations de charge: plus rapide aux heures de pointe, plus lent hors pic, avec un pattern vendredi midi.
  - Usage: alimente Job 2 et Job 3 pour le calcul de demande et le matching taxi-trajet.

## Flink (stream processing)
- Job 1 - GPS Normalizer (week3and4.ipynb ou job1_gps_normalizer.py)
  - Entree: raw.gps (flux brut des GPS).
  - Traitements: parsing, filtrage des points hors Casablanca, enrichment (zone_id), normalisation des champs, anonymisation si besoin.
  - Sorties:
    - Kafka processed.gps (positions propres et coherentes).
    - Cassandra vehicle_positions pour les dashboards temps-reel.

- Job 2 - Demand Aggregator (week3and4.ipynb ou job2_demand_aggregator.py)
  - Entrees: processed.gps (vehicules actifs) + raw.trips (demandes).
  - Traitements: fenetrage temporel, comptage vehicules actifs par zone, comptage demandes en attente, calcul ratio offre/demande.
  - Sorties:
    - Kafka processed.demand (indicateurs de tension par zone).
    - Cassandra demand_zones pour l affichage heatmap et analytics.

- Job 3 - Trip Matcher (week3and4.ipynb ou job3_trip_matcher.py)
  - Entrees: processed.gps + raw.trips.
  - Traitements: etat en memoire des taxis disponibles, selection du meilleur taxi, estimation ETA, mise a jour du statut.
  - Sorties:
    - Kafka processed.trips (resultats de matching).
    - Cassandra trips pour l historique et le suivi des trajets.

## Cassandra (serving layer)
- Tables principales: vehicle_positions, demand_zones, trips
- Role: stocker les donnees propres et servir les dashboards/API

### vehicle_positions (positions GPS normalisees)
- Objectif: garder les positions recentes des taxis, par ville et par zone.
- Exemple de colonnes (vue authentique):
  - city, zone_id, event_time, taxi_id, latitude, longitude, speed, status
- Usage typique: heatmap de taxis actifs, derniere position par zone.

### demand_zones (offre vs demande par fenetre)
- Objectif: suivre la tension offre/demande dans chaque zone.
- Exemple de colonnes (vue authentique):
  - city, zone_id, window_start, active_vehicles, pending_requests, ratio
- Usage typique: carte des zones sous tension, suivi par tranche temporelle.

### trips (matching taxi-trajet)
- Objectif: historiser les demandes et leur matching.
- Exemple de colonnes (vue authentique):
  - city, date_bucket, created_at, trip_id, rider_id, origin_zone, destination_zone, call_type, matched_taxi, eta_seconds, status
- Usage typique: monitoring des matchs, temps d attente, succes/echoues.

## Grafana (visualisation)
- Role: consommer Cassandra pour afficher les heatmaps et tableaux temps-reel

## Notebooks analyses
- DataClean.ipynb: pipeline batch + geomapping + export CSV
- week3and4.ipynb: jobs Flink (1, 2, 3) dans un notebook d execution

---

Ce resume decrit l ensemble des taches realisees et comment chaque composant contribue au workflow final.