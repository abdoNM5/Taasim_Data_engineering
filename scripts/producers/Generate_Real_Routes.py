import pandas as pd
import json
import osmnx as ox
import networkx as nx
import os
import time

GRAPH_FILE = "casablanca_network.graphml"

if __name__ == '__main__':
    print("1. Chargement des données de base de Spark...")
    if not os.path.exists("casablanca_teleported.csv"):
        print("ERREUR : 'casablanca_teleported.csv' introuvable.")
        exit(1)
    
    df = pd.read_csv("casablanca_teleported.csv")
    total_trips = len(df)
    print(f"[{total_trips} trajets à traiter au total]")
    
    print("\n2. Préparation du graphe des routes (Casablanca)...")
    if not os.path.exists(GRAPH_FILE):
        print("-> Téléchargement du graphe depuis OpenStreetMap...")
        # On télécharge le graphe SANS les autoroutes
        G = ox.graph_from_point(
            (33.55, -7.58), 
            dist=25000, 
            custom_filter='["highway"]["area"!~"yes"]["highway"!~"motorway|motorway_link|trunk|trunk_link"]'
        )
        ox.save_graphml(G, GRAPH_FILE)
        print(f"-> Graphe sauvegardé dans '{GRAPH_FILE}'.")
    else:
        print(f"-> Chargement du graphe depuis '{GRAPH_FILE}' en mémoire (patientez un instant)...")
        G = ox.load_graphml(GRAPH_FILE)
        print("-> Graphe chargé avec succès !")
    
    print("\n3. Calcul des vrais trajets sur route (Traitement standard)...")
    print("-> (Le multiprocessing a été désactivé pour éviter de saturer la RAM de ton ordinateur)")
    start_time = time.time()
    
    real_polylines = []
    success_count = 0
    
    for idx, row in df.iterrows():
        try:
            polyline = json.loads(row["CASA_POLYLINE"])
            if not polyline or len(polyline) < 2:
                real_polylines.append("[]")
                continue
            
            start_lon, start_lat = polyline[0]
            end_lon, end_lat = polyline[-1]
            
            orig_node = ox.distance.nearest_nodes(G, start_lon, start_lat)
            dest_node = ox.distance.nearest_nodes(G, end_lon, end_lat)
            
            route = nx.shortest_path(G, orig_node, dest_node, weight="length")
            real_route = [[G.nodes[n]['x'], G.nodes[n]['y']] for n in route]
            
            real_polylines.append(json.dumps(real_route))
            success_count += 1
            
        except Exception:
            real_polylines.append("[]")
            
        # Afficher la progression tous les 100 trajets
        if (idx + 1) % 100 == 0 or (idx + 1) == total_trips:
            print(f"Progression : {idx + 1} / {total_trips} trajets analysés...")
            
    df["REAL_POLYLINE"] = real_polylines
    
    elapsed = time.time() - start_time
    print(f"\nTerminé ! {success_count}/{total_trips} trajets mappés sur des routes réelles en {elapsed:.2f} secondes !")
    
    print("\n4. Nettoyage et Sauvegarde du fichier final...")
    final_df = df[df["REAL_POLYLINE"] != "[]"].copy()
    final_df["CASA_POLYLINE"] = final_df["REAL_POLYLINE"]
    final_df = final_df.drop(columns=["REAL_POLYLINE"])
    
    final_df.to_csv("casablanca_real_roads_final.csv", index=False)
    print("Succès ! Le fichier 'casablanca_real_roads_final.csv' est prêt à être envoyé dans Kafka.")
