#!/usr/bin/env python
from datetime import datetime
import subprocess
import requests
import sys
import os

# On remonte au /home/user pour construire le chemin vers le dossier dim_brest pour les SMS
h_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
send_sms_path = os.path.join(h_directory, "dim_brest", "sms.py")

def log_message(message, level="INFO"):
    """Fonction de log pour afficher un timestamp avec le niveau d'erreur."""
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    print(f"{timestamp} [{level}] -> {message}")

def send_sms(message):
    """Exécute le scrpt sms.py avec le message en argument."""
    subprocess.run([sys.executable, send_sms_path, message])

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
