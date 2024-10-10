#!/usr/bin/env python
import argparse
import pandas as pd
import os
import csv
import re
import functions_anfr

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
        addr_parts = [str(row[col]) for col in ['adresse0', 'adresse1', 'adresse2', 'adresse3'] if pd.notna(row[col])]
        addr = " ".join(addr_parts)
        cc_insee = row['code_insee']
        return addr + " " + conv_insee(cc_insee, insee_data)
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
        functions_anfr.log_message(f"Erreur de parsing lors du chargement du fichier '{file_path}' - {e}", "ERROR")
        return pd.DataFrame()  # Return an empty DataFrame to allow the program to continue
    except Exception as e:
        functions_anfr.log_message(f"Problème lors du chargement du fichier '{file_path}' - {e}", "ERROR")
        return pd.DataFrame()  # Same as above

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
        match = re.match(r"(\D+)\s(\d+)", tech)
        if match:
            technology = match.group(1).strip()
            frequency = int(match.group(2))
            return (tech_order[technology], frequency)
        return (float('inf'), 0)  # Au cas où le format est incorrect, mettre à la fin
    
    # Trier les éléments en utilisant la clé de tri définie
    sorted_elements = sorted(elements, key=sort_key)

    # Reconstituer la ligne
    return ", ".join(sorted_elements)

def merge_and_process(added_path, modified_path, removed_path, output_path, insee_data):
    try:
        # Charger les fichiers CSV
        added_df = preprocess_csv(added_path, 'comp_added.csv')
        modified_df = preprocess_csv(modified_path, 'comp_modified.csv')
        removed_df = preprocess_csv(removed_path, 'comp_removed.csv')

        if added_df.empty or modified_df.empty or removed_df.empty:
            functions_anfr.log_message("Un ou plusieurs fichiers de données sont vides ou n'ont pas été chargés correctement.", "ERROR")
            return

        # Ajouter la colonne 'action'
        added_df['action'] = 'AJO'
        modified_df['action'] = modified_df.apply(determine_action, axis=1)
        removed_df['action'] = 'SUP'

        # Relocalisations
        relocation_df = detect_relocalisation(added_df, removed_df)

        # Détection des changements d'identifiant de support
        id_change_df = detect_id_change(added_df, removed_df)

        # Détection des changements d'adresse
        address_change_df = detect_address_change(added_df, removed_df)

        # Retirer les lignes détectées comme relocalisations, changements d'ID, et changements d'adresse
        added_df = added_df[~added_df['id_support'].isin(pd.concat([relocation_df['id_support'], id_change_df['id_support'], address_change_df['id_support']]))]
        removed_df = removed_df[~removed_df['id_support'].isin(pd.concat([relocation_df['id_support'], id_change_df['id_support'], address_change_df['id_support']]))]

        # Renommer les colonnes pour harmoniser les noms
        added_df = added_df.rename(columns={'adresse_complete': 'adresse'})
        removed_df = removed_df.rename(columns={'adresse_complete': 'adresse'})
        relocation_df = relocation_df.rename(columns={'adresse_added': 'adresse'})
        id_change_df = id_change_df.rename(columns={'id_support_added': 'id_support', 'adresse0': 'adresse0', 'adresse1': 'adresse1', 'adresse2': 'adresse2', 'adresse3': 'adresse3'})
        address_change_df = address_change_df.rename(columns={'id_support_added': 'id_support', 'adresse0': 'adresse0', 'adresse1': 'adresse1', 'adresse2': 'adresse2', 'adresse3': 'adresse3'})

        # Fusionner toutes les données traitées
        final_df = pd.concat([added_df, modified_df, removed_df, relocation_df, id_change_df, address_change_df], ignore_index=True)

        # Appliquer maj_addr avec insee_data préchargé
        final_df['adresse'] = final_df.apply(lambda row: maj_addr(row, insee_data), axis=1)

        final_df = final_df.groupby(['id_support', 'operateur', 'action']).agg({
            'technologie': lambda x: ', '.join(set(x)),  # Utilisation de set() pour éviter les duplications
            'adresse': 'first',
            'code_insee': 'first',
            'coordonnees': 'first',
        }).reset_index()

        final_df['technologie'] = final_df['technologie'].apply(sort_technologies)

        final_df = final_df.sort_values(['id_support', 'operateur', 'action']).reset_index(drop=True)

        # Sauvegarder le fichier final
        final_df.to_csv(output_path, index=False)
        functions_anfr.log_message(f"Fichier final '{output_path}' généré avec succès.")
    except Exception as e:
        functions_anfr.log_message(f"Échec lors du traitement des fichiers - {e}", "FATAL")
        raise SystemExit(1)

  
def detect_relocalisation(added_df, removed_df):
    merged_df = pd.merge(
        added_df, removed_df,
        on=['operateur', 'id_support', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee'],
        suffixes=('_added', '_removed')
    )
    relocalisations = merged_df[merged_df['coordonnees_added'] != merged_df['coordonnees_removed']].copy()

    # Utiliser .loc pour modifier de manière sécurisée
    relocalisations.loc[:, 'action'] = 'CHL'
    relocalisations.loc[:, 'technologie'] = 'Changement de localisation.'

    relocalisations = relocalisations[['id_support', 'operateur', 'action', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee', 'coordonnees_added']]
    relocalisations.rename(columns={'coordonnees_added': 'coordonnees'}, inplace=True)

    return relocalisations

def detect_id_change(added_df, removed_df):
    # Fusionner les DataFrames sur les colonnes appropriées
    merged_df = pd.merge(
        added_df, removed_df,
        on=['operateur', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'coordonnees', 'code_insee'],
        suffixes=('_added', '_removed')
    )

    # Vérifier si la colonne 'id_support_added' existe
    if 'id_support_added' not in merged_df.columns or 'id_support_removed' not in merged_df.columns:
        raise KeyError("Les colonnes 'id_support_added' ou 'id_support_removed' sont manquantes après la fusion.")

    # Sélection des changements d'ID de support
    id_changes = merged_df[merged_df['id_support_added'] != merged_df['id_support_removed']].copy()

    # Utiliser .loc pour modifier de manière sécurisée
    id_changes.loc[:, 'action'] = 'CHI'
    id_changes.loc[:, 'technologie'] = 'Soon.'

    # Garder seulement les colonnes nécessaires
    id_changes = id_changes[['id_support_added', 'operateur', 'action', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee', 'coordonnees']]
    id_changes.rename(columns={'id_support_added': 'id_support'}, inplace=True)

    return id_changes

def detect_address_change(added_df, removed_df):
    merged_df = pd.merge(
        added_df, removed_df,
        on=['id_support', 'operateur', 'technologie', 'coordonnees', 'code_insee'],
        suffixes=('_added', '_removed')
    )
    address_changes = merged_df[
        (merged_df['adresse0_added'] != merged_df['adresse0_removed']) |
        (merged_df['adresse1_added'] != merged_df['adresse1_removed']) |
        (merged_df['adresse2_added'] != merged_df['adresse2_removed']) |
        (merged_df['adresse3_added'] != merged_df['adresse3_removed'])
    ].copy()

    # Utiliser .loc pour modifier de manière sécurisée
    address_changes.loc[:, 'action'] = 'CHA'
    address_changes['technologie'] = 'Soon.'

    address_changes = address_changes[['id_support', 'operateur', 'action', 'technologie', 'adresse0_added', 'adresse1_added', 'adresse2_added', 'adresse3_added', 'code_insee', 'coordonnees']]
    address_changes.rename(columns={'adresse0_added': 'adresse0', 'adresse1_added': 'adresse1', 'adresse2_added': 'adresse2', 'adresse3_added': 'adresse3'}, inplace=True)

    return address_changes

def main(no_insee, no_process, debug):
    """Fonction régissant l'intégralité du programme."""
    path_app = os.path.dirname(os.path.abspath(__file__))
    added_path = os.path.join(path_app, 'files', 'compared', 'comp_added.csv')
    modified_path = os.path.join(path_app, 'files', 'compared', 'comp_modified.csv')
    removed_path = os.path.join(path_app, 'files', 'compared', 'comp_removed.csv')
    insee_path = os.path.join(path_app, 'files', 'cc_insee', 'cc_insee.csv')
    pretraite_path = os.path.join(path_app, 'files', 'pretraite', 'pretraite.csv')

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