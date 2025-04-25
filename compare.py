#!/usr/bin/env python
import argparse
import pandas as pd
import time
import os
from datetime import datetime, timedelta
import requests
import functions_anfr

def download_data(url, save_path, max_retries=3, delay=60):
    """Télécharge les données depuis l'URL spécifiée avec 3 essais en cas d'échec."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=180)
            response.raise_for_status()
            with open(save_path, 'wb') as file:
                file.write(response.content)
            functions_anfr.log_message("Téléchargement des données terminé avec succès.")
            return save_path
        except requests.exceptions.RequestException as e:
            functions_anfr.log_message(f"Tentative {attempt}/{max_retries} échouée - {e}", "WARN")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                functions_anfr.log_message(f"Échec du téléchargement après {max_retries} tentatives.", "ERROR")
                raise SystemExit(1)
    
def csv_files_update(path_new_csv):
    """Détermine les CSV entre lesquels il faut effectuer la comparaison à partir de leurs horodatages."""
    
    # Calcul des dates limites
    date = datetime.now()
    date_limite_sup = date - timedelta(days=1)
    date_limite_inf = date - timedelta(days=30)
    
    # Initialiser la variable pour le fichier le plus récent dans l'intervalle
    old_csv_path = None
    min_diff = timedelta.max
    
    # Parcourir les fichiers dans le répertoire
    for fichier in os.listdir(os.path.dirname(path_new_csv)):
        path_check_file = os.path.join(os.path.dirname(path_new_csv), fichier)
        
        # Extraire l'horodatage du fichier
        try:
            # Extraire uniquement la partie date de la chaîne de caractères
            file_timestamp_str = fichier.split('_')[0]
            file_timestamp = datetime.strptime(file_timestamp_str, "%Y%m%d%H%M%S")
        except ValueError:
            continue  # Ignorer les fichiers qui ne correspondent pas au format attendu

        # Supprimer les fichiers plus vieux que 30 jours
        if file_timestamp < date_limite_inf:
            os.remove(path_check_file)
        
        # Trouver le fichier dans l'intervalle de 1 jour à 30 jours
        elif date_limite_inf <= file_timestamp <= date_limite_sup:
            diff = date - file_timestamp
            if diff < min_diff and path_check_file != path_new_csv:
                min_diff = diff
                old_csv_path = path_check_file
    
    # Si old_csv_path est toujours None (c'est-à-dire aucun fichier trouvé qui diffère de path_new_csv),
    if old_csv_path is None:
        raise FileNotFoundError("Aucun fichier ancien trouvé dans l'intervalle qui corresponde aux critères.")
    
    functions_anfr.send_sms(f"MAJ_ANFR: Comparaison lancée entre : {datetime.strptime(os.path.basename(path_new_csv)[:14], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')} et : {datetime.strptime(os.path.basename(old_csv_path)[:14], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')}. TBC.")
    selected_timestamp = datetime.strptime(os.path.basename(path_new_csv)[:14], '%Y%m%d%H%M%S').strftime('%d/%m/%Y à %H:%M:%S')
    return old_csv_path, path_new_csv, selected_timestamp

def rename_old_file(old_path, new_path):
    """Renomme l'actuel fichier nouveau en ancien, et supprime l'ancien fichier."""
    try:
        if os.path.exists(old_path):
            if os.path.exists(new_path):
                os.remove(new_path)
            os.rename(old_path, new_path)
        functions_anfr.log_message(f"Fichier '{old_path}' renommé en '{new_path}'.")
    except OSError as e:
        functions_anfr.log_message(f"Échec du renommage des fichiers - {e}", "ERROR")

def load_and_process_csv(file_path):
    """Charge le fichier CSV, effectue le prétraitement et renvoie le DataFrame résultant."""
    try:
        df = pd.read_csv(file_path, sep=";")
        # Filtrer les colonnes nécessaires
        df = df[['adm_lb_nom', 'sup_id', 'emr_lb_systeme', 'nat_id', 'sup_nm_haut', 'tpo_id', 'adr_lb_lieu', 'adr_lb_add1', 'adr_lb_add2', 'adr_lb_add3', 'com_cd_insee', 'coordonnees', 'statut']]
        # Renommer les colonnes
        df = df.rename(columns={'adm_lb_nom': 'operateur', 'sup_id': 'id_support', 'emr_lb_systeme': 'technologie', 'nat_id': 'type_support', 'sup_nm_haut': 'hauteur_support', 'tpo_id': 'proprietaire_support', 'adr_lb_lieu': 'adresse0', 'adr_lb_add1': 'adresse1', 'adr_lb_add2': 'adresse2', 'adr_lb_add3': 'adresse3', 'com_cd_insee': 'code_insee', 'coordonnees': 'coordonnees', 'statut': 'statut'})
        return df
    except FileNotFoundError:
        functions_anfr.log_message(f"Le fichier '{file_path}' est introuvable.", "FATAL")
        raise SystemExit(1)
    except pd.errors.ParserError as e:
        functions_anfr.log_message(f"Erreur lors de l'analyse du fichier CSV '{file_path}' - {e}", "FATAL")
        raise SystemExit(1)
    except Exception as e:
        functions_anfr.log_message(f"Problème lors du chargement du fichier CSV '{file_path}' - {e}", "ERROR")
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
        functions_anfr.log_message(f"Clé manquante lors de la comparaison des données - {e}", "ERROR")
        return None, None, None  # On retourne des valeurs vides afin de ne pas casser la suite
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la comparaison des données - {e}", "ERROR")
        return None, None, None

def write_results(df, file_path, message):
    """Écrit les données dans un fichier CSV."""
    try:
        df.to_csv(file_path, index=False)
        nb_rows = len(df)
        functions_anfr.log_message(f"{message} il y a {nb_rows} lignes.")
        return f"{message} il y a {nb_rows} lignes. "
    except IOError as e:
        functions_anfr.log_message(f"Impossible d'écrire dans le fichier '{file_path}' - {e}", "ERROR")

def main(no_file_update, no_download, no_compare, no_write, debug):
    """Fonction main régissant l'intégralité du programme."""
    # Spécifie les chemins des fichiers
    path_app = os.path.dirname(os.path.abspath(__file__))
    download_path = os.path.join(path_app, 'files', 'from_anfr')
    url = "https://data.anfr.fr/d4c/api/records/2.0/downloadfile/format=csv&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da&use_labels_for_header=true"

    # Télécharge les données
    if not no_download:
        filename_from_anfr = functions_anfr.get_filename_from_server(url)
        download_path_r = os.path.join(download_path, filename_from_anfr)
        functions_anfr.log_message("Début du téléchargement du fichier de data.anfr.fr")
        curr_csv_path = download_data(url, download_path_r)
        functions_anfr.log_message("Téléchargement terminé")
    else:
        functions_anfr.log_message("Téléchargement sauté : demandé par argument", "WARN")
        curr_csv_path = "PATHPATHPATHPATHPATH"

    # Détermine les CSV entre lesquels il faut faire la comparaison
    if not no_file_update:
        old_csv_path, current_csv_path, timestamp = csv_files_update(curr_csv_path)
        functions_anfr.log_message(f"Comparaison entre {old_csv_path} et {current_csv_path}")
    else:
        functions_anfr.log_message(f"Mise à jour des fichiers CSV sautée : demandé par argument", "WARN")
        timestamp = ""

    start_time = time.time()

    # Comparaison des données
    if not no_compare:
        functions_anfr.log_message("Début de la comparaison")
        # Charge et traite les anciennes données
        df_old = load_and_process_csv(old_csv_path)
        if debug:
            functions_anfr.log_message("Ancien CSV chargé", "DEBUG")
        # Charge et traite les nouvelles données
        df_current = load_and_process_csv(current_csv_path)
        if debug:
            functions_anfr.log_message("Nouveau CSV chargé", "DEBUG")

        # Compare les données
        df_added, df_removed, df_modified = compare_data(df_old, df_current)
        functions_anfr.log_message("Comparaison terminée")
    else:
        df_added, df_removed, df_modified = None, None, None
        functions_anfr.log_message("Comparaison sautée : demandé par argument", "WARN")

    # Écriture des résultats
    if not no_write:
        functions_anfr.log_message("Début écriture des résultats")
        if df_removed is not None:
            string_sms = "Done. "
            if debug:
                functions_anfr.log_message("Début écriture résultats df_removed", "DEBUG")
            string_sms += write_results(df_removed, os.path.join(path_app, 'files', 'compared', 'comp_removed.csv'), "Lignes supprimées : ")
        if df_modified is not None:
            if debug:
                functions_anfr.log_message("Début écriture résultats df_modified", "DEBUG")
            string_sms += write_results(df_modified, os.path.join(path_app, 'files', 'compared', 'comp_modified.csv'), "Lignes modifiées : ")
        if df_added is not None:
            if debug:
                functions_anfr.log_message("Début écriture résultats df_added", "DEBUG")
            string_sms += write_results(df_added, os.path.join(path_app, 'files', 'compared', 'comp_added.csv'), "Nouvelles lignes : ")
        functions_anfr.log_message("Ecriture des résultats terminée")
        functions_anfr.send_sms(string_sms)
        if all(x is None for x in (df_removed, df_modified, df_added)):
            functions_anfr.log_message("MAJ ANFR vide, fin du programme", "FATAL")
            functions_anfr.send_sms("MAJ vide, annulé.")
            raise SystemExit(1)
        
    else:
        functions_anfr.log_message("Ecriture des résultats sautée : demandé par argument", "WARN")

    with open(os.path.join(path_app, 'files', 'compared', 'timestamp.txt'), 'w') as f:
              f.write(str(timestamp) + "\n")
              f.write(str(old_csv_path) + "\n")
              f.write(str(current_csv_path))
              f.close()

    # Temps d'exécution
    end_time = time.time()
    duration = end_time - start_time
    functions_anfr.log_message(f"La comparaison est terminée et a pris {time.strftime('%H:%M:%S', time.gmtime(duration))} à se faire.")

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