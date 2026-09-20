#!/usr/bin/env python
from datetime import datetime
from pathlib import Path
import re
from zoneinfo import ZoneInfo
import subprocess
import requests
import sys
import os

# On remonte au /home/user pour construire le chemin vers le dossier dim_brest pour les SMS
h_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
send_sms_path = os.path.join(h_directory, "dim_brest", "sms.py")
ANFR_FILENAME_PATTERN = re.compile(r"^(?P<generated>\d{14})_.*?_(?P<data>\d{8})\.csv$")
PARIS_TIMEZONE = ZoneInfo("Europe/Paris")


def log_message(message, level="INFO"):
    """Fonction de log pour afficher un timestamp avec le niveau d'erreur."""
    timestamp = datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    print(f"{timestamp} [{level}] -> {message}")


def parse_anfr_filename(file_path: str):
    """Extrait les timestamps ANFR et convertit la génération UTC en heure locale."""
    match = ANFR_FILENAME_PATTERN.match(Path(file_path).name)
    if not match:
        return None

    generated_utc = datetime.strptime(match.group("generated"), "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo("UTC"))
    generated_local = generated_utc.astimezone(PARIS_TIMEZONE).replace(tzinfo=None)
    data_date = datetime.strptime(match.group("data"), "%Y%m%d").date()
    return generated_utc, generated_local, data_date


def get_period_code(timestamp_str: str, period_type: str, data_date=None) -> str:
    """Génère le code de période selon le type.
    
    Args:
        timestamp_str: Timestamp au format "%d/%m/%Y à %H:%M:%S"
        period_type: Type de période ('hebdo', 'mensu' ou 'trim')
    
    Returns:
        Code de période formaté (S##_YYYY, MM_YYYY ou T#_YYYY)
    """
    dt = datetime.strptime(timestamp_str, "%d/%m/%Y à %H:%M:%S")
    if data_date is not None:
        if isinstance(data_date, str):
            data_date = datetime.strptime(data_date, "%Y-%m-%d").date()
        dt = datetime.combine(data_date, datetime.min.time())
    
    if period_type == "hebdo":
        iso_year, iso_week, _ = dt.isocalendar()
        return f"S{iso_week:02d}_{iso_year}"
    elif period_type == "mensu":
        return f"{dt.month:02d}_{dt.year}"
    elif period_type == "trim":
        trimestre = (dt.month - 1) // 3 + 1
        return f"T{trimestre}_{dt.year}"
    else:
        raise ValueError("Type non reconnu. Utiliser 'hebdo', 'mensu' ou 'trim'.")

def send_sms(message, level="INFO"):
    """Exécute le scrpt sms.py avec le message en argument."""
    subprocess.run([sys.executable, send_sms_path, f"MAJ_ANFR - {level} - {message}"])

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

def detect_separator(file_path: str) -> str:
        """Détecte le séparateur CSV sur la première ligne uniquement."""
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            first_line = f.readline()
        for sep in (';', ','):
            if sep in first_line:
                return sep
        return ','  # fallback