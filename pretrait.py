#!/usr/bin/env python
import argparse
import pandas as pd
import os
import csv
import re
import functions_anfr
import numpy as np
from collections import defaultdict

ZB_TECHNOS = {"LTE 700", "LTE 800", "UMTS 900"}
fc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "compared", "timestamp.txt")
with open(fc_file, "r") as f:
    lines = f.readlines()
    OLD_CSV_PATH = lines[1].strip()
    NEW_CSV_PATH = lines[2].strip()

def load_insee_data(filepath, encoding='utf-8'):
    """Charge les données issues du fichier de concordance entre code postal, nom de ville et code INSEE."""
    insee_data = {}
    try:
        with open(filepath, mode='r', encoding=encoding) as file:
            reader = csv.reader(file, delimiter=';')
            for row in reader:
                if row[0] not in insee_data:
                    insee_data[row[0]] = {
                        'nom_commune': row[1],
                        'code_postal': row[2]
                    }
        functions_anfr.log_message("Données INSEE chargées avec succès.")
    except UnicodeDecodeError:
        functions_anfr.log_message(f"Erreur de décodage avec l'encodage {encoding}.", "ERROR")
    except FileNotFoundError:
        functions_anfr.log_message(f"Fichier INSEE '{filepath}' introuvable.", "FATAL")
        raise SystemExit(1)
    except Exception as e:
        functions_anfr.log_message(f"Problème lors du chargement des données INSEE - {e}", "ERROR")
    return insee_data

def conv_insee(code_insee, insee_data):
    """Convertit un code INSEE en code postal et nom de commune."""
    code_insee_str = str(code_insee).zfill(5)
    if code_insee_str in insee_data:
        commune_info = insee_data[code_insee_str]
        return f"{commune_info['code_postal']} {commune_info['nom_commune']}"
    else:
        return "00404 ERR CONV INSEE"

def maj_addr(row, insee_data):
    """Met à jour l'adresse d'une ligne donnée."""
    try:
        # Filtrer les parties vides de l'adresse
        addr_parts = []
        for col in ['adresse1', 'adresse2', 'adresse3']:
            if pd.notna(row[col]):
                addr_parts.append(str(row[col]))
        
        # Special handling for 'adresse0'
        if pd.notna(row['adresse0']):
            addr_parts.append(f"({str(row['adresse0'])})")
        
        # Join the address parts into a single string
        addr = " ".join(addr_parts)

        cc_insee = row['code_insee']
        return (addr + " " + conv_insee(cc_insee, insee_data)).upper()
    
    except KeyError as e:
        functions_anfr.log_message(f"Clé manquante dans les données de la ligne - {e}", "ERROR")
        return "00404 ERR ADDRESS"

def preprocess_csv(file_path, source):
    """Charge un fichier CSV dans un DataFrame Pandas."""
    try:
        df = pd.read_csv(file_path)
        df['source'] = source
        functions_anfr.log_message(f"Chargement du fichier '{file_path}' terminé avec succès.")
        return df
    except FileNotFoundError:
        functions_anfr.log_message(f"Le fichier '{file_path}' est introuvable.", "FATAL")
        raise SystemExit(1)
    except pd.errors.ParserError as e:
        functions_anfr.log_message(f"Erreur de parsing lors du chargement du fichier '{file_path}' - {e}", "FATAL")
        raise SystemExit(1)
    except Exception as e:
        functions_anfr.log_message(f"Problème lors du chargement du fichier '{file_path}' - {e}", "FATAL")
        raise SystemExit(1)

def determine_action(row):
    """Détermine l'action effectuée par l'opérateur à partir du fichier d'origine."""
    try:
        if row['source'] == 'comp_added.csv':
            return 'AJO'
        elif row['source'] == 'comp_modified.csv':
            if row['statut_old'] == 'Projet approuvé' and (row['statut_last'] == 'Techniquement opérationnel' or row['statut_last'] == 'En service'):
                return 'ALL'
            elif (row['statut_old'] == 'En service' or row['statut_old'] == 'Techniquement opérationnel') and row['statut_last'] == 'Projet approuvé':
                return 'EXT'
        elif row['source'] == 'comp_removed.csv':
            return 'SUP'
    except KeyError as e:
        functions_anfr.log_message(f"Clé manquante pour déterminer l'action - {e}", "ERROR")
        return "UNKNOWN"
    
def sort_technologies(row):
    """Remet les différentes technologies passées en paramètre dans le bon ordre."""
    # Séparer les éléments de la ligne par virgule
    elements = row.split(", ")

    # Définir l'ordre des technologies
    tech_order = {"GSM": 1, "UMTS": 2, "LTE": 3, "5G NR": 4}

    # Fonction de tri
    def sort_key(tech):
        # Extraire la technologie et la fréquence avec une regex
        match = re.match(r"\b((?:GSM|UMTS|LTE))\s(\d{3,4})\b|\b(5G NR)\s(\d{3,5})\b", tech)
        if match:
            technology = match.group(1) or match.group(3)
            frequency = match.group(2) or match.group(4)
            return (tech_order.get(technology, float('inf')), int(frequency))
        return (float('inf'), 0)  # Au cas où le format est incorrect, mettre à la fin
    
    # Trier les éléments en utilisant la clé de tri définie
    sorted_elements = sorted(elements, key=sort_key)

    # Reconstituer la ligne
    return ", ".join(sorted_elements)

def find_and_isolate_duplicates(df: pd.DataFrame, location_threshold=0.001, address_similarity_threshold=0.5):
    """
    Efficiently find and isolate duplicates in the DataFrame based on location, address similarity, 
    and matching technologies/providers, ensuring actions differ.
    
    :param df: DataFrame with columns 'coordonnees', 'adresse', 'technologie', 'operateur', and 'action'.
    :param location_threshold: Maximum allowed difference between coordinates for a match.
    :param address_similarity_threshold: Minimum similarity ratio between addresses for a match.
    :return: DataFrame containing potential duplicates with normalized technologies, matching providers, and differing actions.
    """
    def are_locations_close(coords1: np.ndarray, coords2: np.ndarray, threshold: float) -> np.ndarray:
        """Check which pairs of coordinates are within a given threshold."""
        return np.sqrt(((coords1[:, None, 0] - coords2[None, :, 0]) ** 2) +
                       ((coords1[:, None, 1] - coords2[None, :, 1]) ** 2)) <= threshold

    def address_similarity_matrix(addresses: list) -> np.ndarray:
        """Compute a similarity matrix for addresses."""
        tokens_list = [set(addr.lower().split()) for addr in addresses]
        matrix = np.zeros((len(tokens_list), len(tokens_list)))
        for i, tokens1 in enumerate(tokens_list):
            for j, tokens2 in enumerate(tokens_list):
                if i != j:
                    intersection = tokens1 & tokens2
                    union = tokens1 | tokens2
                    matrix[i, j] = len(intersection) / len(union) if union else 0
        return matrix

    # Normalize technologies into sets for comparison
    df['technologie_set'] = df['technologie'].apply(lambda x: frozenset(x.split(", ")))

    # Normalize coordinates into numpy array
    coords = df['coordonnees'].apply(lambda c: tuple(map(float, c.split(", ")))).to_numpy()
    coords_array = np.array(list(coords))
    
    # Check location closeness
    location_matches = are_locations_close(coords_array, coords_array, location_threshold)

    # Compute address similarity matrix
    address_similarity = address_similarity_matrix(df['adresse'].tolist()) >= address_similarity_threshold

    # Combine location and address similarity
    combined_matches = location_matches & address_similarity

    # Filter matches with same technology, same provider, and differing actions
    tech_provider_action_matches = (
        (df['technologie_set'].to_numpy()[:, None] == df['technologie_set'].to_numpy()[None, :]) &
        (df['operateur'].to_numpy()[:, None] == df['operateur'].to_numpy()[None, :]) &
        (df['action'].to_numpy()[:, None] != df['action'].to_numpy()[None, :])  # Actions must differ
    )

    duplicate_matches = combined_matches & tech_provider_action_matches

    # Collect indices of duplicates
    duplicate_indices = np.where(duplicate_matches)

    # Create a mask to filter the DataFrame
    is_duplicate = np.zeros(len(df), dtype=bool)
    is_duplicate[np.unique(duplicate_indices[0])] = True

    # Drop intermediate column
    df = df.drop(columns=['technologie_set'])
    return df[is_duplicate]  # Return the filtered DataFrame

try:
    df_old = pd.read_csv(OLD_CSV_PATH, sep=";", on_bad_lines="skip", dtype=str)
    df_new = pd.read_csv(NEW_CSV_PATH, sep=";", on_bad_lines="skip", dtype=str)
except Exception as e:
    functions_anfr.log_message(f"Erreur lors de la lecture du CSV : {e}", "ERROR")
    raise

def extract_tech_dict(df):
    """Prépare un DataFrame simplifié pour traitement ultérieur par is_zb()."""
    df_clean = df.dropna(subset=["sup_id", "adm_lb_nom", "emr_lb_systeme"])
    return (
        df_clean.groupby(["sup_id", "adm_lb_nom"])["emr_lb_systeme"]
        .unique()
        .apply(set)
        .to_dict()
    )

techs_new_map = extract_tech_dict(df_new)
techs_old_map = extract_tech_dict(df_old)

def is_zb(support_id, operateur):
    """Détermine si, pour un sup_id et un opé donné, une antenne est une 'Zone Blanche' ou non."""
    key = (str(support_id).strip(), operateur)
    techs_new = techs_new_map.get(key, set())
    techs_old = techs_old_map.get(key, set())

    return (
        (techs_new and techs_new <= ZB_TECHNOS)
        or
        (techs_old and techs_old <= ZB_TECHNOS)
    )

def build_new_status_map(df_old):
    """Prépare un dictionnaire simplifié pour traitement ultérieur par is_new()."""
    df_old_clean = df_old.dropna(subset=["sup_id", "adm_lb_nom", "statut"])
    grouped = df_old_clean.groupby(["sup_id", "adm_lb_nom"])["statut"].apply(list)
    return defaultdict(list, grouped.to_dict())

new_status_dict = build_new_status_map(df_old)

def is_new(support_id, operateur):
    """Détermine si, pour un sup_id et un opé donné, une antenne est 'Nouvelle' ou non."""
    support_id = str(support_id)
    key = (support_id, operateur)
    statuts = new_status_dict.get(key, [])
    if not statuts:
        return True
    return all(stat == "Projet approuvé" for stat in statuts)

def merge_and_process(added_path, modified_path, removed_path, output_path, insee_data):
    """Fusionne trois CSV décrivant les modifications faites par les opérateurs en un seul CSV."""
    try:
        added_df = preprocess_csv(added_path, 'comp_added.csv')
        modified_df = preprocess_csv(modified_path, 'comp_modified.csv')
        removed_df = preprocess_csv(removed_path, 'comp_removed.csv')

        if added_df.empty or modified_df.empty or removed_df.empty:
            functions_anfr.log_message("Un ou plusieurs fichiers de données sont vides ou n'ont pas été chargés correctement.", "FATAL")
            raise SystemExit(1)

        added_df['action'] = 'AJO'
        modified_df['action'] = modified_df.apply(determine_action, axis=1)
        removed_df['action'] = 'SUP'

        final_df = pd.concat([added_df, modified_df, removed_df], ignore_index=True)

        # Uniformiser les colonnes `type_support`, `hauteur_support`, et `proprietaire_support`
        final_df['type_support'] = final_df['type_support_x'].combine_first(final_df['type_support_y'])
        final_df['hauteur_support'] = final_df['hauteur_support_x'].combine_first(final_df['hauteur_support_y'])
        final_df['proprietaire_support'] = final_df['proprietaire_support_x'].combine_first(final_df['proprietaire_support_y'])

        # Appliquer maj_addr avec insee_data préchargé
        final_df['adresse'] = final_df.apply(lambda row: maj_addr(row, insee_data), axis=1)

        # Ajouter les colonnes nécessaires à l'agrégation
        final_df = final_df.groupby(['id_support', 'operateur', 'action']).agg({
            'technologie': ', '.join,
            'adresse': 'first',
            'code_insee': 'first',
            'coordonnees': 'first',
            'type_support': 'first',
            'hauteur_support': 'first',
            'proprietaire_support': 'first',
        }).reset_index()

        final_df['technologie'] = final_df['technologie'].apply(sort_technologies)

        # Dictionnaire de correspondance des types de supports
        correspondances_type_support = {
            0: "Sans nature",
            40: "Sémaphore",
            41: "Phare",
            4: "Château d'eau - réservoir",
            38: "Immeuble",
            39: "Local technique",
            42: "Mât",
            8: "Intérieur galerie",
            9: "Intérieur sous-terrain",
            10: "Tunnel",
            11: "Mât béton",
            12: "Mât métallique",
            21: "Pylône",
            17: "Bâtiment",
            19: "Monument historique",
            20: "Monument religieux",
            22: "Pylône autoportant",
            23: "Pylône autostable",
            24: "Pylône haubané",
            25: "Pylône treillis",
            26: "Pylône tubulaire",
            31: "Silo",
            32: "Ouvrage d'art (pont, viaduc)",
            33: "Tour hertzienne",
            34: "Dalle en béton",
            999999999: "Support non décrit",
            43: "Fût",
            44: "Tour de contrôle",
            45: "Contre-poids au sol",
            46: "Contre-poids sur shelter",
            47: "Support DEFENSE",
            48: "Pylône arbre",
            49: "Ouvrage de signalisation (portique routier, panneau routier)",
            50: "Balise ou bouée",
            51: "XXX",
            52: "Éolienne",
            55: "Mobilier urbain"
        }

        # Transformation type support avec .map()
        final_df['type_support'] = final_df['type_support'].fillna("Inconnu") \
                                      .map(correspondances_type_support) \
                                      .fillna("Inconnu")

        correspondances_proprietaire_support = {
            58: "DAUPHIN TELECOM",
            60: "REUNION NUMERIQUE",
            45: "RATP",
            46: "Titulaire programme Radio/TV",
            47: "Office des Postes et Télécom",
            36: "Altitude Telecom",
            37: "Antalis",
            38: "One Cast",
            40: "Onati",
            41: "France Caraïbes Mobiles",
            42: "FREE-MOBILE",
            43: "Lagardère Active Média",
            44: "Outremer Telecom",
            1: "ANFR",
            2: "Association",
            3: "Aviation Civile",
            4: "BOUYGUES",
            5: "CCI, Ch Métier, Port Aut, Aéroport",
            6: "Conseil Départemental",
            7: "Conseil Régional",
            8: "Coopérative Agricole, Vinicole",
            9: "Copropriété, Syndic, SCI",
            10: "CROSS",
            11: "DDE",
            13: "EDF ou GDF",
            14: "Établissement de soins",
            15: "État, Ministère",
            16: "ORANGE Services Fixes",
            17: "Syndicat des eaux, Adduction",
            18: "État, Ministère",
            19: "La Poste",
            20: "Météo",
            21: "ORANGE",
            22: "Particulier",
            23: "Phares et balises",
            24: "SNCF Réseau",
            25: "RTE",
            26: "SDIS, secours, incendie",
            27: "SFR",
            28: "Société HLM",
            29: "Société Privée",
            30: "Sociétés d'Autoroutes",
            31: "Société Réunionnaise de Radiotéléphonie",
            32: "TDF",
            33: "Towercast",
            34: "Commune, communauté de communes",
            35: "Voies navigables de France",
            39: "État, Ministère",
            49: "BOLLORE",
            48: "9 CEGETEL",
            50: "COMPLETEL",
            51: "DIGICEL",
            52: "EUTELSAT",
            53: "EXPERTMEDIA",
            54: "MEDIASERV",
            55: "BELGACOM",
            59: "Itas Tim",
            56: "AIRBUS",
            57: "GUYANE NUMERIQUE",
            62: "SNCF",
            64: "Pacific Mobile Telecom",
            63: "VITI",
            61: "GLOBECAST",
            69: "ZEOP",
            80: "REGIE HTES PYRENEES HAUT DEBIT",
            76: "TOWEO",
            79: "ONEWEB",
            75: "ON TOWER FRANCE",
            68: "CELLNEX",
            73: "PHOENIX FRANCE INFRASTRUCTURES",
            81: "VALOCIME",
            70: "DGEN",
            72: "HIVORY",
            78: "NEXT TOWER",
            67: "Service des Postes et Télécom",
            65: "ATC FRANCE",
            71: "HUBONE",
            74: "TOTEM",
            77: "Électricité De Tahiti",
            66: "Telco OI",
            12: "Autres"
        }

        # Transformation propriétaire support avec .map()
        final_df['proprietaire_support'] = final_df['proprietaire_support'].fillna("Inconnu") \
                                      .map(correspondances_proprietaire_support) \
                                      .fillna("Inconnu")

        # Transofmration hauteur support
        final_df['hauteur_support'] = final_df['hauteur_support'].fillna(0).apply(lambda x: f"{str(x).replace('.', ',')}m")

        # Trier les valeurs
        final_df = final_df.sort_values(['id_support', 'operateur', 'action']).reset_index(drop=True)

        # Identifier les lignes en doublon
        duplicated = final_df.duplicated(subset=['id_support', 'operateur', 'technologie'], keep=False)

        # Supprimer les doublons
        final_df = final_df[~duplicated]

        duplicates_df = find_and_isolate_duplicates(final_df)

        final_df = final_df[~final_df.index.isin(duplicates_df.index)]

        # On arrondit les coordonnées pour éviter de surcharger le CSV
        final_df['coordonnees'] = (
            final_df['coordonnees']
            .str.split(',', expand=True)
            .astype(float)
            .round(4)
            .astype(str)
            .agg(','.join, axis=1)
        )

        # Liste des fréquences autorisées sur une ligne pour qu'elle soit traitée par is_zb()
        autorisées_zb = {"LTE 700", "LTE 800", "UMTS 900"}

        # Liste des actions autorisées sur une ligne pour qu'elle soit traitée par is_new()
        autorisées_actions = {"ALL", "AJO", "SUP"}

        # Préparation des paires uniques valides pour is_zb()
        valid_zb_rows = final_df[
            final_df["technologie_set"].apply(lambda ts: set(ts).issubset(autorisées_zb))
        ]
        zb_pairs = valid_zb_rows[["id_support", "operateur"]].drop_duplicates()

        # Appel une seule fois par (id_support, opérateur)
        zb_status_map = {
            (row["id_support"], row["operateur"]): is_zb(row["id_support"], row["operateur"])
            for _, row in zb_pairs.iterrows()
        }

        # Application avec fallback à False pour les cas non concernés
        def compute_is_zb(row):
            key = (row["id_support"], row["operateur"])
            return zb_status_map.get(key, False)

        final_df["is_zb"] = final_df.apply(compute_is_zb, axis=1)


        # Préparation des paires uniques valides pour is_new()
        valid_new_rows = final_df[
            final_df["action"].str.strip().str.upper().isin(autorisées_actions)
        ]
        unique_pairs = valid_new_rows[["id_support", "operateur"]].drop_duplicates()

        # Appel une seule fois par (id_support, opérateur)
        new_status_map = {
            (row["id_support"], row["operateur"]): is_new(row["id_support"], row["operateur"])
            for _, row in unique_pairs.iterrows()
        }

        # Application avec fallback à False pour les cas non concernés
        def compute_is_new(row):
            key = (row["id_support"], row["operateur"])
            return new_status_map.get(key, False)

        final_df["is_new"] = final_df.apply(compute_is_new, axis=1)

        # On vire la colonne qui fait doublon de "technologie"
        final_df = final_df.drop('technologie_set', axis=1)

        bouygues_df = final_df.loc[final_df['operateur'] == "BOUYGUES TELECOM"]
        free_df = final_df.loc[(final_df['operateur'] == "FREE MOBILE") | (final_df['operateur'] == "TELCO OI")]
        orange_df = final_df.loc[final_df['operateur'] == "ORANGE"]
        sfr_df = final_df.loc[(final_df['operateur'] == "SFR") | (final_df['operateur'] == "SRR")]

        final_df.to_csv(os.path.join(output_path, 'index.csv'), index=False)
        bouygues_df.to_csv(os.path.join(output_path, 'bouygues.csv'), index=False)
        free_df.to_csv(os.path.join(output_path, 'free.csv'), index=False)
        orange_df.to_csv(os.path.join(output_path, 'orange.csv'), index=False)
        sfr_df.to_csv(os.path.join(output_path, 'sfr.csv'), index=False)

        functions_anfr.log_message(f"Fichiers finaux générés avec succès, duplications supprimées.")
    except Exception as e:
        functions_anfr.log_message(f"Échec lors du traitement des fichiers - {e}", "FATAL")
        raise SystemExit(1)

def main(no_insee, no_process, debug):
    """Fonction régissant l'intégralité du programme."""
    path_app = os.path.dirname(os.path.abspath(__file__))
    added_path = os.path.join(path_app, 'files', 'compared', 'comp_added.csv')
    modified_path = os.path.join(path_app, 'files', 'compared', 'comp_modified.csv')
    removed_path = os.path.join(path_app, 'files', 'compared', 'comp_removed.csv')
    insee_path = os.path.join(path_app, 'files', 'cc_insee', 'cc_insee.csv')
    pretraite_path = os.path.join(path_app, 'files', 'pretraite')

    # Charger les données INSEE une seule fois
    if not no_insee:
        insee_data = load_insee_data(insee_path, encoding='ISO-8859-1')
    else:
        functions_anfr.log_message("Chargement INSEE sauté : demandé par argument", "WARN")
        insee_data = {}

    if not no_process:
        merge_and_process(added_path, modified_path, removed_path, pretraite_path, insee_data)
        functions_anfr.log_message("Prétraitement terminé")
    else:
        functions_anfr.log_message("Prétraitement sauté : demandé par argument", "WARN")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")

    # Ajouter des arguments pour sauter des étapes
    parser.add_argument('--no-insee', action='store_true', help="Ne pas charger les données INSEE, ne pas modifier les adresses.")
    parser.add_argument('--no-process', action='store_true', help="Ne pas effectuer le traitement des données.")
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug.")
    
    args = parser.parse_args()

    main(no_insee=args.no_insee, no_process=args.no_process, debug=args.debug)