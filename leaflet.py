#!/usr/bin/env python
import argparse
import pandas as pd
import os
import folium
from folium import Map, Popup
from folium.plugins import MarkerCluster, Geocoder
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
        Geocoder().add_to(carte)
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
        'FREE CARAIBES' : 'fmb',
        'ZEOP' : 'zop',
        'DIGICEL' : 'dig',
        'GOUV NELLE CALEDONIE (OPT)' : 'opt',
        'OUTREMER TELECOM' : 'ott',
        'PMT/VODAFONE' : 'pmt',
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
            'Suppressions': folium.FeatureGroup(name='Suppressions').add_to(carte)
            #'Changements Adresse': folium.FeatureGroup(name='Changements Adresse').add_to(carte),  # CHA
            #'Changements Localisation': folium.FeatureGroup(name='Changements Localisation').add_to(carte),  # CHL
            #'Changements ID Support': folium.FeatureGroup(name='Changements ID Support').add_to(carte)  # CHI
        }

        clusters = {
            'Ajouts': MarkerCluster(name='Cluster des Ajouts').add_to(feature_groups['Ajouts']),
            'Activations': MarkerCluster(name='Cluster des Activations').add_to(feature_groups['Activations']),
            'Extinctions': MarkerCluster(name='Cluster des Extinctions').add_to(feature_groups['Extinctions']),
            'Suppressions': MarkerCluster(name='Cluster des Suppressions').add_to(feature_groups['Suppressions'])
            #'Changements Adresse': MarkerCluster(name='Cluster des Changements Adresse').add_to(feature_groups['Changements Adresse']),  # CHA
            #'Changements Localisation': MarkerCluster(name='Cluster des Changements Localisation').add_to(feature_groups['Changements Localisation']),  # CHL
            #'Changements ID Support': MarkerCluster(name='Cluster des Changements ID Support').add_to(feature_groups['Changements ID Support'])  # CHI
        }

        folium.LayerControl().add_to(carte)
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la création des groupes de fonctionnalités ou des clusters: {e}", "FATAL")
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

                address = row['adresse']
                footer = row['type_support'] + " - " + row['hauteur_support'] + " - " + row['proprietaire_support']

                for operateur, actions in operateur_data.items():
                    bandeau_texte = f"<a href='https://data.anfr.fr/visualisation/map/?id=observatoire_2g_3g_4g&location=17,{latitude},{longitude}' target='_blank' style='color:#FFFFFF;'>Support n°{row['id_support']}</a>"

                    operator_colors = {
                        "BOUYGUES TELECOM": "#009BCE",
                        "FREE MOBILE": "#6D6E71",
                        "SFR": "#E40012",
                        "ORANGE": "#FD7B02",
                        "TELCO OI": "#6D6E71",
                        "SRR": "#E40012",
                        "FREE CARAIBES" : "#6D6E71",
                        "ZEOP" : "#681260",
                        "DIGICEL" : "#E4002B",
                        "GOUV NELLE CALEDONIE (OPT)" : "#292C83",
                        "OUTREMER TELECOM" : "#DE006F",
                        "PMT/VODAFONE" : "#FF0E00",
                    }
                    
                    bandeau_couleur = operator_colors.get(operateur, "#000000")

                    links_html = f"""
                    <a href='https://cartoradio.fr/index.html#/cartographie/lonlat/{longitude}/{latitude}' target='_blank' class="icone">
                        <img src="https://fraetech.github.io/maj-hebdo/icons/cartoradio.avif" alt="Cartoradio">
                    </a>
                    <a href='https://www.google.fr/maps/place/{latitude},{longitude}' target='_blank' class="icone">
                        <img src="https://fraetech.github.io/maj-hebdo/icons/maps.avif" alt="Google Maps">
                    </a>
                    """
                    if row['operateur'] == 'FREE MOBILE' or row['operateur'] == 'TELCO OI':
                        links_html += f"""
                        <a href='https://rncmobile.net/site/{latitude},{longitude}' target='_blank' class="icone">
                            <img src="https://fraetech.github.io/maj-hebdo/icons/rnc.avif" alt="RNC Mobile">
                        </a>
                        """
                    titre = f"<strong>{address}</strong>"
                    html_content = ""
                    html_content += "<ul>"
                    if actions["ajout"]:
                        html_content += f"<li><strong>Nouvelle(s) fréquence(s) :</strong><br> {', '.join(actions['ajout'])}</li>"
                    if actions["activation"]:
                        html_content += f"<li><strong>Activation fréquence :</strong><br> {', '.join(actions['activation'])}</li>"
                    if actions["extinction"]:
                        html_content += f"<li><strong>Extinction fréquence :</strong><br> {', '.join(actions['extinction'])}</li>"
                    if actions["suppression"]:
                        html_content += f"<li><strong>Suppression fréquence :</strong><br> {', '.join(actions['suppression'])}</li>"
                    if actions["changement adresse"]:
                        html_content += f"<li><strong>Changement d'adresse. Ancienne adresse : </strong><br> {', '.join(actions['changement adresse'])}</li>"
                    if actions["changement localisation"]:
                        html_content += f"<li><strong>Changement de localisation.</strong><br> {', '.join(actions['changement localisation'])}</li>"
                    if actions["changement id support"]:
                        html_content += f"<li><strong>Changement ID support. Ancien ID : </strong><br> {', '.join(actions['changement id support'])}</li>"
                    html_content += "</ul>"

                    icon_path = get_icon_path(operateur, action_label, files_path)

                    popup_html = f"""
                    <div class="bandeau" style="background-color: {bandeau_couleur};">
                        {bandeau_texte}
                    </div>
                    <div class="icone-container">
                        {links_html}
                    </div>
                    <div class="titre">
                        {titre}
                    </div>
                    <div class="contenu">
                        {html_content}
                    </div>
                    <div class="titre">
                        {footer}
                    """
                    marker = folium.Marker(
                        location=coord,
                        popup=Popup(popup_html, max_width=300),
                        tooltip=f"{operateur}",
                        icon=CustomIcon(icon_image=icon_path, icon_size=(48, 48))
                    )
                    marker.add_to(cluster)
        except Exception as e:
            functions_anfr.log_message(f"Erreur lors de l'ajout des marqueurs pour le support {support_id}: {e}", "ERROR")


def ajouter_html(carte, timestamp):
    date_h_gen = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    custom_html = f"""
<title>Modifications ANFR hebdomadaires</title>
<head>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Didact+Gothic&display=swap" rel="stylesheet">
<link rel="icon" href="https://fraetech.github.io/maj-hebdo/icons/favicon.svg" type="image/svg+xml" alt="Favicon">
</head>
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
    <a href='https://fraetech.github.io/maj-hebdo/' target="_self">Tous</a>,
    <a href='https://fraetech.github.io/maj-hebdo/bouygues.html' target="_self">Carte Bouygues</a>,
    <a href='https://fraetech.github.io/maj-hebdo/free.html' target="_self">Carte Free</a>,
    <a href='https://fraetech.github.io/maj-hebdo/orange.html' target="_self">Carte Orange</a>,
    <a href='https://fraetech.github.io/maj-hebdo/sfr.html' target="_self">Carte SFR</a><br>
    <b>Source :</b> <a href='https://data.anfr.fr/visualisation/information/?id=observatoire_2g_3g_4g' target='_blank'>OpenData ANFR</a> | <small>Carte générée le {date_h_gen} - v25.03.21</small></p>
</div>"""

    custom_html += """
<style>
.bandeau {
    color: white;
    text-align: center;
    padding: 5px;
    font-size: 1.2em;
    font-weight: bold;
    line-height: 1.2;
}

* {
  font-family: "Didact Gothic", sans-serif !important;
  font-weight: 500;
  font-style: normal;
}

.icone-container {
    display: flex;
    justify-content: center;
    gap: 10px;
    margin: 10px 0;
}

.icone {
    width: 40px;
    height: 40px;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: #f0f0f0;
    border-radius: 6px;
    cursor: pointer;
    transition: transform 0.2s, background-color 0.2s;
}

.icone:hover {
    background-color: #007BFF;
    transform: scale(1.1);
}

.icone img {
    width: 20px;
    height: 20px;
}

.titre {
    text-align: center;
    font-size: 1em;
    margin: 5px 0;
    color: #333;
}

.contenu {
    padding: 10px;
    background-color: #f9f9f9;
}
</style>
"""

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

    with open(os.path.join(path_app, 'files', 'compared', 'timestamp.txt'), "r") as f:
        timestamp = str(f.read())
        f.close

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

        ajouter_html(my_map, timestamp)

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
