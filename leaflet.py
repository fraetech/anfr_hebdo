#!/usr/bin/env python
import argparse
import pandas as pd
import os
import folium
from folium import Map, Marker, Popup
from folium.plugins import MarkerCluster
from folium.features import CustomIcon
from datetime import datetime

def log_message(message):
    """Fonction de log pour affichier un timestamp."""
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    print(f"{timestamp} -> {message}")

def charger_donnees(pretraite_path):
    """Charge les données depuis un CSV dans un DataFrame Pandas."""
    if not os.path.exists(pretraite_path):
        raise FileNotFoundError(f"Le fichier {pretraite_path} est introuvable.")
    try:
        pretraite_df = pd.read_csv(pretraite_path, sep=',')
    except Exception as e:
        log_message(f"FATAL: Erreur lors du chargement du fichier CSV: {e}")
        raise
    return pretraite_df

def creer_carte():
    """Créé la carte Folium."""
    try:
        carte = Map(location=[48.8566, 2.3522], zoom_start=6)
    except Exception as e:
        log_message(f"FATAL: Erreur lors de la création de la carte: {e}")
        raise
    return carte

def get_icon_path(operateur, action_label, files_path):
    """Fonction pour obtenir le chemin de l'icône personnalisée en fonction de l'opérateur et de l'action."""
    icon_directory = os.path.join(files_path, 'icons')  # Dossier où sont stockées les icônes
    
    operateur_map = {
        'ORANGE': 'ora',
        'FREE MOBILE': 'fmb',
        'SFR': 'sfr',
        'BOUYGUES TELECOM': 'byt',
        'TELCO OI': 'fmb',
        'SRR': 'sfr',
        'MISC': 'misc'
    }

    if operateur not in operateur_map.keys():
        operateur = 'MISC'

    action_map = {
        'Ajout': 'ajo',
        'Activation': 'act',
        'Extinction': 'ext',
        'Suppression': 'sup'
    }

    icon_filename = f"{operateur_map[operateur].lower()}_{action_map[action_label].lower()}.png"
    icon_path = os.path.join(icon_directory, icon_filename)
    
    if not os.path.exists(icon_path):
        try:
            icon_filename = f"misc_{action_map[action_label].lower()}.png"
            icon_path = os.path.join(icon_directory, icon_filename)
        except Exception as e:
            log_message(f"FATAL: Impossible de trouver l'icône {icon_filename}: {e}")
            raise
    return icon_path

def ajouter_marqueurs(dataframe, carte, files_path):
    """Ajout les marqueurs à la carte Folium."""
    try:
        feature_groups = {
            'Ajouts': folium.FeatureGroup(name='Ajouts').add_to(carte),
            'Activations': folium.FeatureGroup(name='Activations').add_to(carte),
            'Extinctions': folium.FeatureGroup(name='Extinctions').add_to(carte),
            'Suppressions': folium.FeatureGroup(name='Suppressions').add_to(carte)
        }

        clusters = {
            'Ajouts': MarkerCluster(name='Cluster des Ajouts').add_to(feature_groups['Ajouts']),
            'Activations': MarkerCluster(name='Cluster des Activations').add_to(feature_groups['Activations']),
            'Extinctions': MarkerCluster(name='Cluster des Extinctions').add_to(feature_groups['Extinctions']),
            'Suppressions': MarkerCluster(name='Cluster des Suppressions').add_to(feature_groups['Suppressions'])
        }

        folium.LayerControl().add_to(carte)
    except Exception as e:
        log_message(f"FATAL: Erreur lors de la création des groupes de fonctionnalités ou des clusters: {e}")
        raise

    support_ids = set()

    for _, row in dataframe.iterrows():
        try:
            support_id = row['id_support']
            if support_id not in support_ids:
                support_ids.add(support_id)
                support_rows = dataframe[dataframe['id_support'] == support_id]

                coord = [float(x) for x in row['coordonnees'].split(',')]
                latitude, longitude = coord
                latitude = round(latitude, 4)
                longitude = round(longitude, 4)

                operateur_data = {}
                for _, support_row in support_rows.iterrows():
                    operateur = support_row['operateur']
                    action_label = "Ajout" if support_row['action'] == 'AJO' else "Activation" if support_row['action'] == 'ALL' else "Extinction" if support_row['action'] == 'EXT' else "Suppression"

                    if operateur not in operateur_data:
                        operateur_data[operateur] = {"ajout": [], "activation": [], "extinction": [], "suppression": []}

                    if action_label == "Ajout":
                        operateur_data[operateur]["ajout"].append(support_row['technologie'])
                        cluster = clusters['Ajouts']
                    elif action_label == "Activation":
                        operateur_data[operateur]["activation"].append(support_row['technologie'])
                        cluster = clusters['Activations']
                    elif action_label == "Extinction":
                        operateur_data[operateur]["extinction"].append(support_row['technologie'])
                        cluster = clusters['Extinctions']
                    elif action_label == "Suppression":
                        operateur_data[operateur]["suppression"].append(support_row['technologie'])
                        cluster = clusters['Suppressions']

                address = row['adresse']
                for operateur, actions in operateur_data.items():
                    html_content = f"<strong>{address}</strong><br>"
                    html_content += f"Support n°{row['id_support']}<br>"
                    html_content += f"<a href='https://cartoradio.fr/index.html#/cartographie/lonlat/{longitude}/{latitude}' target='_blank'>Voir sur Cartoradio</a><br>"
                    if row['operateur'] == 'FREE MOBILE' or row['operateur'] == 'TELCO OI':
                        html_content += f"<a href='https://rncmobile.net/site/{latitude},{longitude}' target='_blank'>Voir sur RNC Mobile</a><br>"
                    html_content += f"<a href='https://data.anfr.fr/visualisation/map/?id=observatoire_2g_3g_4g&location=17,{latitude},{longitude}' target='_blank'>Voir sur data.anfr.fr</a>"

                    html_content += "<ul>"
                    if actions["ajout"]:
                        html_content += f"<li><strong>Nouvelles fréquences :</strong> {', '.join(actions['ajout'])}</li>"
                    if actions["activation"]:
                        html_content += f"<li><strong>Activation fréquence :</strong> {', '.join(actions['activation'])}</li>"
                    if actions["extinction"]:
                        html_content += f"<li><strong>Extinction fréquence :</strong> {', '.join(actions['extinction'])}</li>"
                    if actions["suppression"]:
                        html_content += f"<li><strong>Suppression fréquence :</strong> {', '.join(actions['suppression'])}</li>"
                    html_content += "</ul>"

                    # Obtenir le chemin de l'icône personnalisée
                    icon_path = get_icon_path(operateur, action_label, files_path)

                    marker = folium.Marker(
                        location=coord,
                        popup=Popup(html_content, max_width=300),
                        tooltip=f"{operateur}",
                        icon=CustomIcon(icon_image=icon_path, icon_size=(48, 48))  # Utiliser l'icône personnalisée
                    )
                    marker.add_to(cluster)
        except Exception as e:
            log_message(f"ERROR: Erreur lors de l'ajout des marqueurs pour le support {support_id}: {e}")

def enregistrer_carte(carte, nom_fichier):
    """Enregistre la carte Folium dans un le fichier HTML final."""
    try:
        carte.save(nom_fichier)
    except Exception as e:
        log_message(f"FATAL: Erreur lors de la sauvegarde de la carte: {e}")
        raise

def main(no_load, no_create_map, no_indicators, no_save_map, debug):
    """Fonction principale régissant le programme."""
    # Spécifie les chemins des fichiers
    path_app = os.path.dirname(os.path.abspath(__file__))
    files_path = os.path.join(path_app, 'files')
    pretraite_path = os.path.join(files_path, 'pretraite', 'pretraite.csv')
    carte_path = os.path.join(files_path, 'out', 'index.html')

    try:
        if not no_load:
            pretraite_df = charger_donnees(pretraite_path)
            log_message('INFO: Fichier CSV pretraite chargé')
        else:
            log_message(f' Chargement fichier pretraite sauté : demandé par argument')

        if not no_create_map:
            my_map = creer_carte()
            if debug:
                log_message('DEBUG: Carte créée')
        else:
            log_message(f' Création de la carte sautée : demandé par argument')

        if not no_indicators:
            ajouter_marqueurs(pretraite_df, my_map, files_path)
            log_message('INFO: Marqueurs ajoutés sur la carte')
        else:
            log_message(f' Ajout des marqueurs sur la carte sauté : demandé par argument')

        if not no_save_map:
            enregistrer_carte(my_map, carte_path)
            log_message('INFO: Carte enregistrée')
        else:
            log_message(f' La sauvegarde de la carte a été sautée : demandé par argument')
    except FileNotFoundError as e:
        log_message(f"FATAL: Un fichier requis est introuvable: {e}")
    except Exception as e:
        log_message(f"FATAL: Une erreur inattendue est survenue: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")

    # Ajouter des arguments pour sauter des étapes
    parser.add_argument('--no-load', action='store_true', help="Ne pas charger les données prétraitées.")
    parser.add_argument('--no-create-map', action='store_true', help="Ne pas créer de carte.")
    parser.add_argument('--no-indicators', action='store_true', help="Ne pas ajouter de marqueurs sur la carte.")
    parser.add_argument('--no-save-map', action='store_true', help="Ne pas sauvegarder la carte.")
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug.")

    args = parser.parse_args()
    main(no_load=args.no_load, no_create_map=args.no_create_map, no_indicators=args.no_indicators, no_save_map=args.no_save_map, debug=args.debug)