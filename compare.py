import argparse
import pandas as pd
import time
import os
import shutil
from datetime import datetime, timedelta
import requests

def log_message(message):
    """Fonction de log pour affichier un timestamp."""
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    print(f"{timestamp} -> {message}")

def download_data(url, save_path):
    """Télécharge les données depuis l'URL spécifiée et les sauvegarde dans le fichier indiqué."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            file.write(response.content)
        log_message(f"INFO: Téléchargement des données terminé avec succès.")
    except requests.exceptions.RequestException as e:
        log_message(f"FATAL: Échec du téléchargement des données - {e}")
        raise SystemExit(1)
    
def csv_files_update(path_new_csv):
    """Détermine les CSV entre lesquels il faut effectuer la comparaison à partir de leurs horodatages."""
    # Obtenir l'horodatage actuel
    actual_timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
    
    # Renommer le fichier en maj_dd_MM_yyyy_hh_mm_ss.csv
    new_name_curr_csv = f"maj_{actual_timestamp}.csv"
    current_csv_path = os.path.join(os.path.dirname(path_new_csv), new_name_curr_csv)
    shutil.move(path_new_csv, current_csv_path)
    
    # Calcul des dates limites
    date = datetime.now()
    date_limite_sup = date - timedelta(days=2)
    date_limite_inf = date - timedelta(days=12)
    
    # Initialiser la variable pour le fichier le plus récent dans l'intervalle
    old_csv_path = None
    min_diff = timedelta.max
    
    # Parcourir les fichiers dans le répertoire
    for fichier in os.listdir(os.path.dirname(current_csv_path)):
        path_check_file = os.path.join(os.path.dirname(current_csv_path), fichier)
        
        # Extraire l'horodatage du fichier
        file_timestamp = datetime.strptime(fichier[4:-4], "%d_%m_%Y_%H_%M_%S")
        
        # Supprimer les fichiers plus vieux que 12 jours
        if file_timestamp < date_limite_inf:
            os.remove(path_check_file)
        
        # Trouver le fichier dans l'intervalle de 2 jours à 12 jours
        elif date_limite_inf <= file_timestamp <= date_limite_sup:
            diff = date - file_timestamp
            if diff < min_diff:
                min_diff = diff
                old_csv_path = path_check_file
    
    return old_csv_path, current_csv_path

def rename_old_file(old_path, new_path):
    """Renomme l'actuel fichier nouveau en ancien, et supprime l'ancien fichier."""
    try:
        if os.path.exists(old_path):
            if os.path.exists(new_path):
                os.remove(new_path)
            os.rename(old_path, new_path)
        log_message(f"INFO: Fichier '{old_path}' renommé en '{new_path}'.")
    except OSError as e:
        log_message(f"ERROR: Échec du renommage des fichiers - {e}")

def load_and_process_csv(file_path):
    """Charge le fichier CSV, effectue le prétraitement et renvoie le DataFrame résultant."""
    try:
        df = pd.read_csv(file_path, sep=";")
        # Filtrer les colonnes nécessaires
        df = df[['adm_lb_nom', 'sup_id', 'emr_lb_systeme', 'adr_lb_lieu', 'adr_lb_add1', 'adr_lb_add2', 'adr_lb_add3', 'com_cd_insee', 'coordonnees', 'statut']]
        # Renommer les colonnes
        df = df.rename(columns={'adm_lb_nom': 'operateur', 'sup_id': 'id_support', 'emr_lb_systeme': 'technologie', 'adr_lb_lieu': 'adresse0', 'adr_lb_add1': 'adresse1', 'adr_lb_add2': 'adresse2', 'adr_lb_add3': 'adresse3', 'com_cd_insee': 'code_insee', 'coordonnees': 'coordonnees', 'statut': 'statut'})
        return df
    except FileNotFoundError:
        log_message(f"FATAL: Le fichier '{file_path}' est introuvable.")
        raise SystemExit(1)
    except pd.errors.ParserError as e:
        log_message(f"FATAL: Erreur lors de l'analyse du fichier CSV '{file_path}' - {e}")
        raise SystemExit(1)
    except Exception as e:
        log_message(f"ERROR: Problème lors du chargement du fichier CSV '{file_path}' - {e}")
        return None

def compare_data(df_old, df_current):
    """Compare les données entre deux DataFrames et retourne les modifications."""
    try:
        df_old['statut_old'] = df_old['statut']
        df_current['statut_last'] = df_current['statut']
        df_merged = pd.merge(df_old, df_current, on=['operateur', 'id_support', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee', 'coordonnees'], how='outer')

        df_added = df_merged[df_merged['statut_old'].isna()]
        df_removed = df_merged[df_merged['statut_last'].isna()]

        df_modified = df_merged[(df_merged['statut_old'] != df_merged['statut_last'])]
        df_modified = df_modified.drop(df_removed.index)
        df_modified = df_modified.drop(df_added.index)

        return df_added, df_removed, df_modified
    except KeyError as e:
        log_message(f"ERROR: Clé manquante lors de la comparaison des données - {e}")
        return None, None, None  # On retourne des valeurs vides afin de ne pas casser la suite
    except Exception as e:
        log_message(f"ERROR: Erreur lors de la comparaison des données - {e}")
        return None, None, None

def write_results(df, file_path, message):
    """Écrit les données dans un fichier CSV."""
    try:
        df.to_csv(file_path, index=False)
        nb_rows = len(df)
        log_message(f"{message} Il y a {nb_rows} lignes.")
    except IOError as e:
        log_message(f"ERROR: Impossible d'écrire dans le fichier '{file_path}' - {e}")

def main(no_file_update, no_download, no_compare, no_write, debug):
    """Fonction main régissant l'intégralité du programme."""
    # Spécifie les chemins des fichiers
    path_app = os.path.dirname(os.path.abspath(__file__))
    download_path = os.path.join(path_app, 'files', 'from_anfr', 'maj_last.csv')

    # Télécharge les données
    if not no_download:
        log_message('INFO: Début du téléchargement du fichier de data.anfr.fr')
        download_data("https://data.anfr.fr/api/records/2.0/downloadfile/format=csv&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da&use_labels_for_header=true", download_path)
        log_message('INFO: Téléchargment terminé')
    else:
        log_message(f' Téléchargement sauté : demandé par argument')

    # Détermine les CSV entre lesquels il faut faire la comparaison
    if not no_file_update:
        old_csv_path, current_csv_path = csv_files_update(download_path)
        log_message(f'INFO: Comparaison entre {old_csv_path} et {current_csv_path}')
    else:
        log_message(f' Mise à jour des fichiers CSV sautée : demandé par argument')

    start_time = time.time()

    # Comparaison des données
    if not no_compare:
        log_message('INFO: Début de la comparaison')
        # Charge et traite les anciennes données
        df_old = load_and_process_csv(old_csv_path)
        if debug:
            log_message('DEBUG: Ancien CSV chargé')
        # Charge et traite les nouvelles données
        df_current = load_and_process_csv(current_csv_path)
        if debug:
            log_message('DEBUG: Nouveau CSV chargé')

        # Compare les données
        df_added, df_removed, df_modified = compare_data(df_old, df_current)
        log_message('INFO: Comparaison terminée')
    else:
        df_added, df_removed, df_modified = None, None, None
        log_message(f' Comparaison sautée : demandé par argument')

    # Écriture des résultats
    if not no_write:
        log_message('INFO: Début écriture des résultats')
        if df_removed is not None:
            if debug:
                log_message('DEBUG: Début écriture résultats df_removed')
            write_results(df_removed, os.path.join(path_app, 'files', 'compared', 'comp_removed.csv'), "Lignes supprimées : ")
        if df_modified is not None:
            if debug:
                log_message('DEBUG: Début écriture résultats df_modified')
            write_results(df_modified, os.path.join(path_app, 'files', 'compared', 'comp_modified.csv'), "Lignes modifiées : ")
        if df_added is not None:
            if debug:
                log_message('DEBUG: Début écriture résultats df_added')
            write_results(df_added, os.path.join(path_app, 'files', 'compared', 'comp_added.csv'), "Nouvelles lignes : ")
        log_message('INFO: Ecriture des résultats terminée')
    else:
        log_message(f' Ecriture des résultats sautée : demandé par argument')

    # Temps d'exécution
    end_time = time.time()
    duration = end_time - start_time
    log_message(f"La comparaison est terminée et a pris {time.strftime('%H:%M:%S', time.gmtime(duration))} à se faire.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")
    
    # Ajouter des arguments pour sauter des étapes
    parser.add_argument('--no-file-update', action='store_true', help="Ne pas maj les fichiers CSV présents dans le répertoire from_anfr.")
    parser.add_argument('--no-download', action='store_true', help="Ne pas télécharger les nouvelles données.")
    parser.add_argument('--no-compare', action='store_true', help="Ne pas comparer les données.")
    parser.add_argument('--no-write', action='store_true', help="Ne pas écrire les résultats.")
    
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debogage.")

    args = parser.parse_args()
    
    main(no_file_update=args.no_file_update, no_download=args.no_download, no_compare=args.no_compare, no_write=args.no_write, debug=args.debug)