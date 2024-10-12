#!/usr/bin/env python
import argparse
import pandas as pd
import os
import folium
import sys
from folium import Map, Marker, Popup
from folium.plugins import MarkerCluster
from folium.features import CustomIcon
from datetime import datetime
from branca.element import MacroElement
from jinja2 import Template
from datetime import datetime
import functions_anfr

def charger_donnees(pretraite_path, operateur):
    """Charge les données depuis un CSV dans un DataFrame Pandas."""
    final_path = os.path.join(pretraite_path, f'{operateur}.csv')
    if not os.path.exists(final_path):
        raise FileNotFoundError(f"Le fichier {final_path} est introuvable.")
    try:
        pretraite_df = pd.read_csv(final_path, sep=',')
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors du chargement du fichier CSV: {e}", "FATAL")
        raise
    return pretraite_df

def creer_carte():
    """Créé la carte Folium."""
    try:
        carte = Map(location=[48.8566, 2.3522], zoom_start=6)
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la création de la carte: {e}", "FATAL")
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
        'Suppression': 'sup',
        'Changement Adresse': 'cha',  # Ajout de CHA
        'Changement Localisation': 'chl',  # Ajout de CHL
        'Changement ID Support': 'chi'  # Ajout de CHI
    }

    icon_filename = f"{operateur_map[operateur].lower()}_{action_map[action_label].lower()}.avif"
    icon_path = os.path.join(icon_directory, icon_filename)
    
    if not os.path.exists(icon_path):
        try:
            icon_filename = f"misc_{action_map[action_label].lower()}.avif"
            icon_path = os.path.join(icon_directory, icon_filename)
        except Exception as e:
            functions_anfr.log_message(f"Impossible de trouver l'icône {icon_filename}: {e}", "FATAL")
            raise
    return icon_path

def ajouter_marqueurs(dataframe, carte, files_path):
    """Ajout les marqueurs à la carte Folium."""
    try:
        feature_groups = {
            'Ajouts': folium.FeatureGroup(name='Ajouts').add_to(carte),
            'Activations': folium.FeatureGroup(name='Activations').add_to(carte),
            'Extinctions': folium.FeatureGroup(name='Extinctions').add_to(carte),
            'Suppressions': folium.FeatureGroup(name='Suppressions').add_to(carte),
            'Changements Adresse': folium.FeatureGroup(name='Changements Adresse').add_to(carte),  # CHA
            'Changements Localisation': folium.FeatureGroup(name='Changements Localisation').add_to(carte),  # CHL
            'Changements ID Support': folium.FeatureGroup(name='Changements ID Support').add_to(carte)  # CHI
        }

        clusters = {
            'Ajouts': MarkerCluster(name='Cluster des Ajouts').add_to(feature_groups['Ajouts']),
            'Activations': MarkerCluster(name='Cluster des Activations').add_to(feature_groups['Activations']),
            'Extinctions': MarkerCluster(name='Cluster des Extinctions').add_to(feature_groups['Extinctions']),
            'Suppressions': MarkerCluster(name='Cluster des Suppressions').add_to(feature_groups['Suppressions']),
            'Changements Adresse': MarkerCluster(name='Cluster des Changements Adresse').add_to(feature_groups['Changements Adresse']),  # CHA
            'Changements Localisation': MarkerCluster(name='Cluster des Changements Localisation').add_to(feature_groups['Changements Localisation']),  # CHL
            'Changements ID Support': MarkerCluster(name='Cluster des Changements ID Support').add_to(feature_groups['Changements ID Support'])  # CHI
        }

        folium.LayerControl().add_to(carte)
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la création des groupes de fonctionnalités ou des clusters: {e}", "FATAL")
        raise

    support_ids = set()

    for _, row in dataframe.iterrows():
        #try:
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
                    action_label = "Ajout" if support_row['action'] == 'AJO' else \
                                "Activation" if support_row['action'] == 'ALL' else \
                                "Extinction" if support_row['action'] == 'EXT' else \
                                "Suppression" if support_row['action'] == 'SUP' else \
                                "Changement Adresse" if support_row['action'] == 'CHA' else \
                                "Changement Localisation" if support_row['action'] == 'CHL' else \
                                "Changement ID Support" if support_row['action'] == 'CHI' else "Suppression"

                    if operateur not in operateur_data:
                        operateur_data[operateur] = {"ajout": [], "activation": [], "extinction": [], "suppression": [],
                                                    "changement adresse": [], "changement localisation": [], "changement id support": []}

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
                    elif action_label == "Changement Adresse":
                        operateur_data[operateur]["changement adresse"].append(support_row['technologie'])
                        cluster = clusters['Changements Adresse']
                    elif action_label == "Changement Localisation":
                        operateur_data[operateur]["changement localisation"].append(support_row['technologie'])
                        cluster = clusters['Changements Localisation']
                    elif action_label == "Changement ID Support":
                        operateur_data[operateur]["changement id support"].append(support_row['technologie'])
                        cluster = clusters['Changements ID Support']

                # Ajout du contenu HTML avec les nouvelles catégories
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
                    if actions["changement adresse"]:
                        html_content += f"<li><strong>Changement d'adresse. Ancienne adresse : </strong> {', '.join(actions['changement adresse'])}</li>"
                    if actions["changement localisation"]:
                        html_content += f"<li><strong>Changement de localisation.</strong> {', '.join(actions['changement localisation'])}</li>"
                    if actions["changement id support"]:
                        html_content += f"<li><strong>Changement ID support. Ancien ID : </strong> {', '.join(actions['changement id support'])}</li>"
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
        #except Exception as e:
        #    functions_anfr.log_message(f"Erreur lors de l'ajout des marqueurs pour le support {support_id}: {e}", "ERROR")


def ajouter_html(carte):
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    custom_html = f"""
<title>Modifications ANFR hebdomadaires</title>
<link rel="icon" href="https://fraetech.github.io/maj-hebdo/icons/favicon.svg" type="image/svg+xml" alt="Favicon">
<div id="message" style="position: fixed; 
            bottom: 50px; left: 50px; 
            z-index: 1000; 
            background-color: white; 
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);">
    <button id="closeButton" style="position: absolute; 
            top: 5px; right: 5px; 
            background-color: transparent; 
            border: none; 
            font-size: 18px; 
            cursor: pointer;">&times;</button>
    <h4><b>Carte MAJ ANFR du {timestamp}</b></h4>
    <p>Vous pouvez choisir les actions à afficher à l'aide du layer control (en haut à droite).<br>
    Questions, remarques et suggestions -> <a href='https://github.com/fraetech/maj-hebdo/issues' target='_blank'>GitHub MAJ_Hebdo</a>.<br>
    Vous cherchez plutôt les modifications d'un opérateur en particulier ?<br>
    <a href='https://fraetech.github.io/maj-hebdo/bouygues.html' target="_self">Carte Bouygues</a>,
    <a href='https://fraetech.github.io/maj-hebdo/free.html' target="_self">Carte Free</a>,
    <a href='https://fraetech.github.io/maj-hebdo/orange.html' target="_self">Carte Orange</a>,
    <a href='https://fraetech.github.io/maj-hebdo/sfr.html' target="_self">Carte SFR</a><br>
    <b>Source :</b> <a href='https://data.anfr.fr/visualisation/information/?id=observatoire_2g_3g_4g' target='_blank'>OpenData ANFR</a> | v24.10.12</p>
</div>"""
    
    custom_html += """
<script>
    document.getElementById('closeButton').onclick = function() {
        document.getElementById('message').style.display = 'none';
    };
</script>
"""
    class CustomElement(MacroElement):
        def __init__(self, html):
            super().__init__()
            self._name = "CustomElement"
            self.html = html

        def render(self, **kwargs):
            template = Template(self.html)
            return template.render()
    carte.get_root().html.add_child(CustomElement(custom_html))

def enregistrer_carte(carte, nom_fichier, operateur):
    """Enregistre la carte Folium dans un le fichier HTML final."""
    final_path = os.path.join(nom_fichier, f'{operateur}.html')
    try:
        carte.save(final_path)
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la sauvegarde de la carte: {e}", "FATAL")
        raise

def main(no_load, no_create_map, no_indicators, no_save_map, operateur, debug):
    """Fonction principale régissant le programme."""
    # Spécifie les chemins des fichiers
    path_app = os.path.dirname(os.path.abspath(__file__))
    files_path = os.path.join(path_app, 'files')
    pretraite_path = os.path.join(files_path, 'pretraite')
    carte_path = os.path.join(files_path, 'out')

    try:
        if not no_load:
            pretraite_df = charger_donnees(pretraite_path, operateur)
            functions_anfr.log_message("Fichier CSV pretraite chargé")
        else:
            functions_anfr.log_message("Chargement fichier pretraite sauté : demandé par argument", "WARN")

        if not no_create_map:
            my_map = creer_carte()
            if debug:
                functions_anfr.log_message("Carte créée", "DEBUG")
        else:
            functions_anfr.log_message(f' Création de la carte sautée : demandé par argument')

        if not no_indicators:
            ajouter_marqueurs(pretraite_df, my_map, files_path)
            functions_anfr.log_message("Marqueurs ajoutés sur la carte")
        else:
            functions_anfr.log_message("Ajout des marqueurs sur la carte sauté : demandé par argument", "WARN")

        ajouter_html(my_map)

        if not no_save_map:
            enregistrer_carte(my_map, carte_path, operateur)
            functions_anfr.log_message(f"Carte enregistrée : {operateur}")
        else:
            functions_anfr.log_message("La sauvegarde de la carte a été sautée : demandé par argument", "WARN")
    except FileNotFoundError as e:
        functions_anfr.log_message(f"Un fichier requis est introuvable: {e}", "FATAL")
    except Exception as e:
        functions_anfr.log_message(f"Une erreur inattendue est survenue: {e}", "FATAL")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")

    # Ajouter des arguments pour sauter des étapes
    parser.add_argument('--no-load', action='store_true', help="Ne pas charger les données prétraitées.")
    parser.add_argument('--no-create-map', action='store_true', help="Ne pas créer de carte.")
    parser.add_argument('--no-indicators', action='store_true', help="Ne pas ajouter de marqueurs sur la carte.")
    parser.add_argument('--no-save-map', action='store_true', help="Ne pas sauvegarder la carte.")
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug.")
    parser.add_argument('--operateur', type=str, default='index', help='Si un opérateur en particulier doit être généré.')

    args = parser.parse_args()
    main(no_load=args.no_load, no_create_map=args.no_create_map, no_indicators=args.no_indicators, no_save_map=args.no_save_map, operateur=args.operateur, debug=args.debug)