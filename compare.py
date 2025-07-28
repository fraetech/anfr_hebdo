#!/usr/bin/env python
import argparse
import pandas as pd
import time
import os
from datetime import datetime, timedelta
import requests
import functions_anfr

def download_data(url, save_path, max_retries=3, delay=60):
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

def get_previous_period_filename(update_type):
    now = datetime.now()
    if update_type == "mensu":
        first_day_last_month = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
        return f"{first_day_last_month.strftime('%m_%Y')}.csv"
    elif update_type == "trim":
        current_quarter = (now.month - 1) // 3 + 1
        last_quarter = current_quarter - 1 if current_quarter > 1 else 4
        year = now.year if current_quarter > 1 else now.year - 1
        return f"T{last_quarter}_{year}.csv"
    return None

def csv_files_update(path_new_csv, update_type):
    dir_path = os.path.dirname(path_new_csv)
    date = datetime.now()
    old_csv_path = None

    if update_type == "hebdo":
        date_limite_sup = date - timedelta(days=1)
        date_limite_inf = date - timedelta(days=31)
        min_diff = timedelta.max
        for fichier in os.listdir(dir_path):
            path_check_file = os.path.join(dir_path, fichier)
            try:
                file_timestamp_str = fichier.split('_')[0]
                file_timestamp = datetime.strptime(file_timestamp_str, "%Y%m%d%H%M%S")
            except ValueError:
                continue

            if file_timestamp < date_limite_inf:
                os.remove(path_check_file)
            elif date_limite_inf <= file_timestamp <= date_limite_sup:
                diff = date - file_timestamp
                if diff < min_diff and path_check_file != path_new_csv:
                    min_diff = diff
                    old_csv_path = path_check_file
    else:
        expected_filename = get_previous_period_filename(update_type)
        #expected_filename = "02_2025.csv"
        #expected_filename = "T1_2025.csv"
        
        # Trouver le fichier de la période précédente
        for fichier in os.listdir(dir_path):
            if fichier == expected_filename:
                old_csv_path = os.path.join(dir_path, fichier)
                break
        
        # Supprimer les fichiers antérieurs à la période précédente
        def is_older_file(filename, reference_filename):
            # Gestion des fichiers mensuels (format: MM_YYYY.csv)
            if reference_filename[0].isdigit() and "_" in reference_filename:
                ref_month = int(reference_filename.split("_")[0])
                ref_year = int(reference_filename.split("_")[1].split(".")[0])
                
                # Vérifier si le fichier est au format mensuel
                if filename[0].isdigit() and "_" in filename:
                    try:
                        month = int(filename.split("_")[0])
                        year = int(filename.split("_")[1].split(".")[0])
                        # Le fichier est plus ancien si l'année est plus petite ou si même année mais mois plus petit
                        return (year < ref_year) or (year == ref_year and month < ref_month)
                    except (ValueError, IndexError):
                        return False
            
            # Gestion des fichiers trimestriels (format: TX_YYYY.csv)
            elif reference_filename.startswith("T") and "_" in reference_filename:
                ref_quarter = int(reference_filename[1:2])
                ref_year = int(reference_filename.split("_")[1].split(".")[0])
                
                # Vérifier si le fichier est au format trimestriel
                if filename.startswith("T") and "_" in filename:
                    try:
                        quarter = int(filename[1:2])
                        year = int(filename.split("_")[1].split(".")[0])
                        # Le fichier est plus ancien si l'année est plus petite ou si même année mais trimestre plus petit
                        return (year < ref_year) or (year == ref_year and quarter < ref_quarter)
                    except (ValueError, IndexError):
                        return False
            
            return False
        
        # Supprimer tous les fichiers plus anciens
        for fichier in os.listdir(dir_path):
            if fichier.endswith(".csv") and is_older_file(fichier, expected_filename):
                file_to_delete = os.path.join(dir_path, fichier)
                try:
                    os.remove(file_to_delete)
                    functions_anfr.log_message(f"Fichier supprimé : {fichier}")
                except Exception as e:
                    functions_anfr.log_message(f"Erreur lors de la suppression de {fichier}: {e}", "ERROR")

    if old_csv_path is None:
        raise FileNotFoundError("Aucun fichier de référence trouvé pour le type de mise à jour spécifié.")

    functions_anfr.send_sms(f"Comparaison lancée entre : {datetime.strptime(os.path.basename(path_new_csv)[:14], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')} et : {datetime.strptime(os.path.basename(old_csv_path)[:14], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')}")
    selected_timestamp = datetime.strptime(os.path.basename(path_new_csv)[:14], '%Y%m%d%H%M%S').strftime('%d/%m/%Y à %H:%M:%S')
    return old_csv_path, path_new_csv, selected_timestamp

def rename_old_file(old_path, new_path):
    try:
        if os.path.exists(old_path):
            if os.path.exists(new_path):
                os.remove(new_path)
            os.rename(old_path, new_path)
        functions_anfr.log_message(f"Fichier '{old_path}' renommé en '{new_path}'.")
    except OSError as e:
        functions_anfr.log_message(f"Échec du renommage des fichiers - {e}", "ERROR")

def load_and_process_csv(file_path):
    try:
        df = pd.read_csv(file_path, sep=",")
        df = df[['adm_lb_nom', 'sup_id', 'emr_lb_systeme', 'nat_id', 'sup_nm_haut', 'tpo_id', 'adr_lb_lieu', 'adr_lb_add1', 'adr_lb_add2', 'adr_lb_add3', 'com_cd_insee', 'coordonnees', 'statut']]
        df = df.rename(columns={
            'adm_lb_nom': 'operateur',
            'sup_id': 'id_support',
            'emr_lb_systeme': 'technologie',
            'nat_id': 'type_support',
            'sup_nm_haut': 'hauteur_support',
            'tpo_id': 'proprietaire_support',
            'adr_lb_lieu': 'adresse0',
            'adr_lb_add1': 'adresse1',
            'adr_lb_add2': 'adresse2',
            'adr_lb_add3': 'adresse3',
            'com_cd_insee': 'code_insee',
            'coordonnees': 'coordonnees',
            'statut': 'statut'
        })
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
        return None, None, None
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la comparaison des données - {e}", "ERROR")
        return None, None, None

def write_results(df, file_path, message):
    try:
        df.to_csv(file_path, index=False, sep=",")
        nb_rows = len(df)
        functions_anfr.log_message(f"{message}{nb_rows}.")
        return f"{message}{nb_rows}."
    except IOError as e:
        functions_anfr.log_message(f"Impossible d'écrire dans le fichier '{file_path}' - {e}", "ERROR")

def main(no_file_update, no_download, no_compare, no_write, old_csv_name, new_csv_name, timestamp_a, debug, update_type):
    path_app = os.path.dirname(os.path.abspath(__file__))
    download_path = os.path.join(path_app, 'files', 'from_anfr')
    url = "https://data.anfr.fr/d4c/api/records/2.0/downloadfile/format=csv&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da&use_labels_for_header=true"

    if not no_download and not new_csv_name:
        filename_from_anfr = functions_anfr.get_filename_from_server(url)
        download_path_r = os.path.join(download_path, filename_from_anfr)
        functions_anfr.log_message("Début du téléchargement du fichier de data.anfr.fr")
        curr_csv_path = download_data(url, download_path_r)
        functions_anfr.log_message("Téléchargement terminé")
    else:
        if no_download:
            functions_anfr.log_message("Téléchargement sauté : demandé par argument", "WARN")
            curr_csv_path = r"PATHPATHPATH"
        else:
            curr_csv_path = os.path.join(download_path, new_csv_name)
            functions_anfr.log_message(f"Vous forcez la MAJ avec le current_csv : {curr_csv_path}", "INFO")

    if not no_file_update and not old_csv_name:
        old_csv_path, current_csv_path, timestamp = csv_files_update(curr_csv_path, update_type)
        functions_anfr.log_message(f"Comparaison entre {old_csv_path} et {current_csv_path}")
    else:
        if no_file_update:
            current_csv_path = curr_csv_path
            timestamp = "15/12/2020 à 13:37:37"
            functions_anfr.log_message(f"Mise à jour des fichiers CSV sautée : vous n'irez pas loin ainsi...", "ERROR")
        else:
            current_csv_path = curr_csv_path
            old_csv_path = os.path.join(download_path, old_csv_name)
            timestamp = str(timestamp_a).strip('\"')
            functions_anfr.log_message(f"Vous forcez la MAJ avec le old_csv : {old_csv_path}", "INFO")
            functions_anfr.log_message(f"Vous forcez la MAJ avec le timestamp : {timestamp}", "INFO")
            
    start_time = time.time()

    if not no_compare:
        functions_anfr.log_message(f"Début de la comparaison entre {old_csv_path} & {current_csv_path}")
        df_old = load_and_process_csv(old_csv_path)
        if debug:
            functions_anfr.log_message("Ancien CSV chargé", "DEBUG")
        df_current = load_and_process_csv(current_csv_path)
        if debug:
            functions_anfr.log_message("Nouveau CSV chargé", "DEBUG")
        df_added, df_removed, df_modified = compare_data(df_old, df_current)
        functions_anfr.log_message("Comparaison terminée")
    else:
        df_added, df_removed, df_modified = None, None, None
        functions_anfr.log_message("Comparaison sautée : demandé par argument", "WARN")

    if not no_write:
        functions_anfr.log_message("Début écriture des résultats")
        string_sms = ""
        if df_removed is not None:
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
        if any(x.empty for x in (df_removed, df_modified, df_added)):
            functions_anfr.log_message("MAJ ANFR vide, fin du programme", "FATAL")

            # Ajouter le nom du fichier courant à ignores.txt
            with open(os.path.join(path_app, 'files', 'ignores.txt'), "a", encoding="utf-8") as f:
                f.write(os.path.basename(curr_csv_path) + "\n")

            # Envoyer les SMS
            functions_anfr.send_sms(string_sms, "INFO")
            functions_anfr.send_sms("MAJ vide... (sûr au moins 1 df)", "FATAL")

            # Supprimer le fichier de la MAJ vide
            try:
                os.remove(curr_csv_path)
                functions_anfr.log_message(f"Fichier supprimé : {curr_csv_path}", "INFO")
            except Exception as e:
                functions_anfr.log_message(f"Erreur lors de la suppression du fichier : {e}", "ERROR")

            raise SystemExit(1)
        else:
            functions_anfr.send_sms(string_sms, "INFO")

    else:
        functions_anfr.log_message("Ecriture des résultats sautée : demandé par argument", "WARN")

    with open(os.path.join(path_app, 'files', 'compared', 'timestamp.txt'), 'w', encoding="utf-8") as f1:
        f1.write(str(timestamp) + "\n")
        f1.write(str(old_csv_path) + "\n")
        f1.write(str(current_csv_path))
        f1.close()
    with open(os.path.join(path_app, 'files', 'pretraite', 'timestamp.txt'), 'w', encoding="utf-8") as f2:
        f2.write(str(timestamp))
        f2.close()

    end_time = time.time()
    duration = end_time - start_time
    functions_anfr.log_message(f"La comparaison est terminée et a pris {time.strftime('%H:%M:%S', time.gmtime(duration))} à se faire.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")
    parser.add_argument('--no-file-update', action='store_true')
    parser.add_argument('--no-download', action='store_true')
    parser.add_argument('--no-compare', action='store_true')
    parser.add_argument('--no-write', action='store_true')
    parser.add_argument('--old-csv-name', type=str, help="Nom de l'ancien fichier CSV avec lequel faire la MAJ")
    parser.add_argument('--new-csv-name', type=str, help="Nom du nouveau fichier CSV avec lequel faire la MAJ, préciser --timestamp SVP")
    parser.add_argument('--timestamp', type=str, help="Timestamp à donner à la MAJ")
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('update_type', choices=["hebdo", "mensu", "trim"])
    args = parser.parse_args()

    main(
        no_file_update=args.no_file_update,
        no_download=args.no_download,
        no_compare=args.no_compare,
        no_write=args.no_write,
        old_csv_name=args.old_csv_name,
        new_csv_name=args.new_csv_name,
        timestamp_a=args.timestamp,
        debug=args.debug,
        update_type=args.update_type
    )