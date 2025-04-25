#!/usr/bin/env python
import sys
import shutil
from pathlib import Path
from dotenv import load_dotenv
import os
import subprocess
import functions_anfr

fc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "compared", "timestamp.txt")
with open(fc_file, "r") as f:
    lines = f.readlines()
    TIMESTAMP = lines[0].strip()

# Charger le token depuis .env
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    functions_anfr.log_message("GITHUB_TOKEN non défini dans le fichier .env.", "FATAL")
    sys.exit(1)

# Vérifier l'argument
if len(sys.argv) != 2 or sys.argv[1] not in {"hebdo", "mensu", "trim"}:
    functions_anfr.log_message("Usage : python github.py [hebdo|mensu|trim]", "FATAL")
    sys.exit(1)

update_type = sys.argv[1]
path_app = Path(__file__).resolve().parent
source_dir = path_app / "files" / "pretraite"
repo_dir = path_app.parent / "maj-hebdo"
dest_dir = repo_dir / "files" / update_type
files = ["index.csv", "bouygues.csv", "free.csv", "orange.csv", "sfr.csv"]

# Créer le dossier cible
dest_dir.mkdir(parents=True, exist_ok=True)

# Copier les fichiers
for file_name in files:
    src = source_dir / file_name
    dst = dest_dir / file_name
    if src.exists():
        shutil.copy2(src, dst)
        functions_anfr.log_message(f"Copié : {src} → {dst}", "INFO")
    else:
        functions_anfr.log_message(f"Fichier manquant : {src}", "WARN")

# Commit & Push
try:
    subprocess.run(["git", "-C", str(repo_dir), "add", str(dest_dir)], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", f"Mise à jour {update_type} du {TIMESTAMP}"], check=True)

    subprocess.run(
    ["git", "-C", str(repo_dir), "push", f"https://{GITHUB_TOKEN}@github.com/fraetech/maj-hebdo"],
        check=True
    )
    functions_anfr.log_message("Modifications poussées sur GitHub.", "INFO")
    functions_anfr.send_sms("Poussé sur GitHub avec succès.")
except subprocess.CalledProcessError as e:
    functions_anfr.log_message(f"Erreur lors du git : {e}", "ERROR")
    sys.exit(1)