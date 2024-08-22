#!/usr/bin/env python
import requests
from bs4 import BeautifulSoup
import os
import subprocess
import sys
from datetime import datetime

def log_message(message):
    """Fonction de log pour affichier un timestamp."""
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    print(f"{timestamp} -> {message}")

def run_script(script_name):
    """Exécute un script Python avec des arguments optionnels."""
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        log_message(f"FATAL: Le script {script_name} a échoué avec le code de retour {e.returncode}. Erreur: {e}")
        sys.exit(e.returncode)
    except Exception as e:
        log_message(f"FATAL: Une erreur inattendue est survenue lors de l'exécution de {script_name}: {e}")
        sys.exit(1)

def check_and_execute_script(url, file_path, script_to_execute):
    """Détermine si une MAJ a eu lieu en se basant sur le nombre de lignes présentes sur la page data.anfr.fr, et lance le script si besoin est."""
    try:
        # Étape 1 : Charger le nombre de lignes depuis le fichier
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                old_nombre_lignes = int(file.read().strip())
        else:
            old_nombre_lignes = 0  # Valeur par défaut si le fichier n'existe pas

        # Étape 2 : Envoyer une requête pour obtenir le contenu HTML de la page
        response = requests.get(url)
        response.raise_for_status()  # Lève une exception si la requête échoue

        # Étape 3 : Analyser le contenu HTML avec BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Étape 4 : Rechercher tous les <span> dans le document
        spans = soup.find_all('span')
        nombre_lignes = None

        for span in spans:
            if 'Nombre de lignes' in span.text:
                # Extraire le nombre à l'intérieur de la balise <b>
                b_tag = span.find('b')
                if b_tag:
                    nombre_lignes = int(b_tag.text.strip())  # Convertir en entier
                    break

        if nombre_lignes is not None:
            log_message(f"INFO: Nombre de lignes actuel : {nombre_lignes}")
            log_message(f"INFO: Nombre de lignes précédent : {old_nombre_lignes}")

            # Étape 5 : Comparer les nombres de lignes et exécuter le script si nécessaire
            if nombre_lignes > old_nombre_lignes:
                log_message("INFO: Le nombre de lignes a augmenté. Démarrage de la MAJ...")
                run_script(script_to_execute)

                # Mettre à jour le fichier avec le nouveau nombre de lignes
                with open(file_path, 'w') as file:
                    file.write(str(nombre_lignes))
            else:
                log_message("INFO: Le nombre de lignes n'a pas augmenté. La MAJ ne sera pas exécutée.")
        else:
            log_message("ERROR: Le nombre de lignes n'a pas été trouvé dans la page.")
    
    except requests.exceptions.RequestException as e:
        log_message(f"ERROR: Erreur lors de l'accès à la page : {e}")
    except ValueError as e:
        log_message(f"ERROR: Erreur de conversion du nombre de lignes : {e}")
    except Exception as e:
        log_message(f"ERROR: Une erreur inattendue s'est produite : {e}")

# Exemple d'utilisation de la fonction
url = "https://data.anfr.fr/visualisation/information/?id=observatoire_2g_3g_4g"
# Spécifie les chemins des fichiers
path_app = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(path_app, 'files', 'api', 'from_html_nb_rows.txt')
script_to_execute = os.path.join(path_app, 'core.py')

check_and_execute_script(url, file_path, script_to_execute)