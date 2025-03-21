#!/usr/bin/env python
import os
import sys
import subprocess
import re
import signal
import functions_anfr

def run_script(script_name):
    """Exécute un script Python avec des arguments optionnels."""
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        functions_anfr.log_message(f"Le script {script_name} a échoué avec le code de retour {e.returncode}. Erreur: {e}", "ERROR")
        return e.returncode
    except Exception as e:
        functions_anfr.log_message(f"Une erreur inattendue est survenue lors de l'exécution de {script_name}: {e}", "ERROR")
        return 1

def timeout_handler(signum, frame):
    """Gestionnaire de signal pour arrêter l'exécution en cas de dépassement du délai."""
    raise TimeoutError("Le temps d'exécution alloué a été dépassé.")

def check_and_execute(url, local_csv_dir, script_to_execute, timeout=300):
    """Vérifie la présence du fichier localement et exécute le script core.py si le fichier n'est pas présent,
       avec un délai maximal d'exécution."""
    try:
        # Définir le gestionnaire de timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)  # Déclencher l'alarme pour le temps limite

        # Récupérer le nom du fichier sur le serveur
        filename = functions_anfr.get_filename_from_server(url)
        local_csv_path = os.path.join(local_csv_dir, filename)

        # Définir le pattern à respecter
        pattern = r'^\d{14}_observatoire(_2g)?(_3g)?(_4g)?(_5g)?\.csv$'

        # Vérifier si le nom du fichier respecte le pattern
        if not re.match(pattern, filename):
            functions_anfr.log_message(f"Le nom de fichier '{filename}' ne respecte pas le pattern requis.", "ERROR")
            return  # Sortir de la fonction sans exécuter le script

        # Vérifier si le fichier est déjà présent localement
        if os.path.exists(local_csv_path):
            functions_anfr.log_message(f"Le fichier {filename} est déjà présent. Aucun téléchargement nécessaire.")
        else:
            functions_anfr.log_message(f"Le fichier {filename} n'est pas présent. Exécution de {script_to_execute}...")
            return_code = run_script(script_to_execute)
            if return_code != 0:
                functions_anfr.log_message(f"L'exécution de {script_to_execute} a échoué avec le code de retour {return_code}.", "ERROR")
            else:
                functions_anfr.log_message(f"Le script {script_to_execute} a été exécuté avec succès.")

        signal.alarm(0)  # Annuler l'alarme si tout s'est bien passé
    except TimeoutError:
        functions_anfr.log_message("Le temps d'exécution maximum a été dépassé.", "CRITICAL")
        functions_anfr.send_sms("Erreur : Temps d'exécution dépassé.")
        sys.exit(1)
    except Exception as e:
        functions_anfr.log_message(f"Une erreur s'est produite : {e}", "CRITICAL")
        functions_anfr.send_sms(f"Erreur : {e}")
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