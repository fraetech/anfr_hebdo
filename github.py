#!/usr/bin/env python
import os
import sys
import shutil
import argparse
import subprocess
import datetime
from pathlib import Path
from dotenv import load_dotenv
import functions_anfr

def get_timestamp():
    fc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "compared", "timestamp.txt")
    with open(fc_file, "r", encoding="utf-8") as f:
        return f.readline().strip()

def get_period_code(timestamp_str: str, type: str) -> str:
    dt = datetime.datetime.strptime(timestamp_str, "%d/%m/%Y à %H:%M:%S")

    if type == "hebdo":
        iso_week, _ = dt.isocalendar()
        return f"S{iso_week:02d}_{dt.year}"
    elif type == "mensu":
        return f"{dt.month:02d}_{dt.year}"
    elif type == "trim":
        trimestre = (dt.month - 1) // 3 + 1
        return f"T{trimestre}_{dt.year}"
    else:
        raise ValueError("Type non reconnu. Utiliser 'hebdo', 'mensu' ou 'trim'.")

def copy_files(update_type: str, path_app: Path, period_code: str):
    source_dir = path_app / "files" / "pretraite"
    repo_dir = path_app.parent / "maj-hebdo"
    dest_dir = repo_dir / "files" / update_type
    if update_type == "hebdo":
        files = ["index.csv", "bouygues.csv", "free.csv", "orange.csv", "sfr.csv", f"{period_code}.csv", "timestamp.txt", f"{period_code}.txt"]
    else:
        files = [f"{period_code}.csv", f"{period_code}.txt"]

    dest_dir.mkdir(parents=True, exist_ok=True)

    for file_name in files:
        src = source_dir / file_name
        dst = dest_dir / file_name
        if src.exists():
            shutil.copy2(src, dst)
            functions_anfr.log_message(f"Copié : {src} → {dst}", "INFO")
        else:
            functions_anfr.log_message(f"Fichier manquant : {src}", "WARN")

    return repo_dir, dest_dir

def git_push(repo_dir: Path, dest_dir: Path, timestamp: str, update_type: str, github_token: str):
    try:
        history_file = repo_dir / "files" / "history.csv"
        subprocess.run(["git", "-C", str(repo_dir), "add", str(dest_dir), str(history_file)], check=True)
        subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", f"Mise à jour {update_type} du {timestamp}"], check=True)
        subprocess.run([
            "git", "-C", str(repo_dir), "push",
            f"https://{github_token}@github.com/fraetech/maj-hebdo"
        ], check=True)
        functions_anfr.log_message("Modifications poussées sur GitHub.", "INFO")
        functions_anfr.send_sms("Poussé sur GitHub avec succès.")
    except subprocess.CalledProcessError as e:
        functions_anfr.log_message(f"Erreur lors du git : {e}", "ERROR")
        sys.exit(1)

def clean(path_app : Path):
    pretraite_path = path_app / "files" / "pretraite"
    for filename in os.listdir(pretraite_path):
        file_path = os.path.join(pretraite_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            functions_anfr.log_message(f"Fichier supprimé : {filename}", "INFO")
    compared_path = path_app / "files" / "compared"
    
    for filename in os.listdir(compared_path):
        file_path = os.path.join(compared_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            functions_anfr.log_message(f"Fichier supprimé : {filename}", "INFO")

def main(args):
    # Charger les variables d'environnement
    load_dotenv()
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        functions_anfr.log_message("GITHUB_TOKEN non défini dans le fichier .env.", "FATAL")
        sys.exit(1)

    timestamp = get_timestamp()
    period_code = get_period_code(timestamp, args.update_type)
    path_app = Path(__file__).resolve().parent

    # Copier les fichiers
    repo_dir, dest_dir = copy_files(args.update_type, path_app, period_code)

    # Git push
    git_push(repo_dir, dest_dir, timestamp, args.update_type, github_token)

    # Clean de pretraite
    clean(path_app)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publier les fichiers ANFR vers le dépôt GitHub.")
    parser.add_argument('update_type', choices=["hebdo", "mensu", "trim"], help="Type de mise à jour")
    args = parser.parse_args()
    main(args)
