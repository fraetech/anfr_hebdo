#!/usr/bin/env python
import requests
import os
import sys
import subprocess
from datetime import datetime

def log_message(message, level="INFO"):
    """Fonction de log pour afficher un timestamp avec le niveau d'erreur."""
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    print(f"{timestamp} [{level}] -> {message}")

def run_script(script_name):
    """Exécute un script Python avec des arguments optionnels."""
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        log_message(f"Le script {script_name} a échoué avec le code de retour {e.returncode}. Erreur: {e}", "ERROR")
        return e.returncode
    except Exception as e:
        log_message(f"Une erreur inattendue est survenue lors de l'exécution de {script_name}: {e}", "ERROR")
        return 1

def get_filename_from_server(url):
    """Récupère le nom du fichier depuis l'URL du serveur."""
    try:
        response = requests.head(url, allow_redirects=True)
        response.raise_for_status()
        content_disposition = response.headers.get('content-disposition')
        if content_disposition:
            filename = content_disposition.split("filename=")[-1].strip('"')
        else:
            filename = response.url.split("/")[-1]
        return filename
    except requests.exceptions.RequestException as e:
        log_message(f"Échec de la récupération du nom du fichier depuis le serveur : {e}", "ERROR")
        raise

def check_and_execute(url, local_csv_dir, script_to_execute):
    """Vérifie la présence du fichier localement et exécute le script core.py si le fichier n'est pas présent."""
    try:
        # Récupérer le nom du fichier sur le serveur
        filename = get_filename_from_server(url)
        local_csv_path = os.path.join(local_csv_dir, filename)

        # Vérifier si le fichier est déjà présent localement
        if os.path.exists(local_csv_path):
            log_message(f"Le fichier {filename} est déjà présent. Aucun téléchargement nécessaire.")
        else:
            log_message(f"Le fichier {filename} n'est pas présent. Exécution de {script_to_execute}...")
            # Exécuter le script de mise à jour (core.py)
            return_code = run_script(script_to_execute)
            if return_code != 0:
                log_message(f"L'exécution de {script_to_execute} a échoué avec le code de retour {return_code}.", "ERROR")
            else:
                log_message(f"Le script {script_to_execute} a été exécuté avec succès.")
    except Exception as e:
        log_message(f"Une erreur s'est produite : {e}", "CRITICAL")
        sys.exit(1)

def main():
    """Fonction principale du programme."""
    # Paramètres du programme
    url = "https://data.anfr.fr/d4c/api/records/2.0/downloadfile/format=csv&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da&use_labels_for_header=true"
    path_app = os.path.dirname(os.path.abspath(__file__))
    local_csv_folder = os.path.join(path_app, 'files', 'from_anfr')
    script_to_execute = os.path.join(path_app, 'core.py')

    # Vérification et exécution
    check_and_execute(url, local_csv_folder, script_to_execute)

if __name__ == "__main__":
    main()