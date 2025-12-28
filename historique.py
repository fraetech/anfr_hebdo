#!/usr/bin/env python
import argparse
import os
import csv
import locale
import shutil
import functions_anfr
from datetime import datetime
from pathlib import Path

locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')

fc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "compared", "timestamp.txt")
with open(fc_file, "r", encoding="utf-8") as f:
    lines = f.readlines()
    TIMESTAMP = lines[0].strip()


def build_label_and_path(period_code: str, dt: datetime, type_: str):
    if type_ == "hebdo":
        iso_year, iso_week, _ = dt.isocalendar()
        label = f"Semaine {iso_week} - {iso_year}"
        path = f"hebdo/{period_code}"
    elif type_ == "mensu":
        mois_nom = dt.strftime("%B").capitalize()
        label = f"{mois_nom} - {dt.year}"
        path = f"mensu/{period_code[:2]}_{dt.year}"
    elif type_ == "trim":
        trimestre = (dt.month - 1) // 3 + 1
        label = f"Trimestre {trimestre} - {dt.year}"
        path = f"trim/{period_code}"
    else:
        raise ValueError("Type non reconnu.")
    
    return label, path

def update_history_csv(type_: str, timestamp_str: str):
    dt = datetime.strptime(timestamp_str, "%d/%m/%Y à %H:%M:%S")
    period_code = functions_anfr.get_period_code(timestamp_str, type_)
    label, path = build_label_and_path(period_code, dt, type_)

    path_app = Path(__file__).resolve().parent
    repo_dir = path_app.parent / "fraetech.github.io"

    history_path = os.path.join(repo_dir, "files", "history.csv")
    existing_rows = []

    # Lire les lignes existantes
    if os.path.exists(history_path):
        with open(history_path, mode="r", encoding="utf-8", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_rows.append(row)
                if row["type"] == type_ and row["path"] == path:
                    functions_anfr.log_message("Entrée déjà présente, aucune modification.", "INFO")
                    return  # Ne rien faire si déjà présent

    # Ajouter une nouvelle ligne
    with open(history_path, mode="a", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["type", "label", "path"])
        if os.stat(history_path).st_size == 0:  # Fichier vide
            writer.writeheader()
        writer.writerow({"type": type_, "label": label, "path": path})
        functions_anfr.log_message(f"Ajouté : {type_},{label},{path}")

def main(args):
    update_history_csv(args.update_type, TIMESTAMP)

    dt = datetime.strptime(TIMESTAMP, "%d/%m/%Y à %H:%M:%S")
    path_app = Path(__file__).resolve().parent
    target_dir = path_app / "files" / "from_anfr"
    source_file = lines[2].strip()

    for period_type in ["hebdo", "mensu", "trim"]:
        period_code = functions_anfr.get_period_code(TIMESTAMP, period_type)
        with open(os.path.join(path_app, "files", "pretraite", f"{period_code}.txt"), "w", encoding="utf-8") as f:
            f.write(str(TIMESTAMP))
            f.close()
        output_filename = f"{period_code}.csv"
        full_path = target_dir / output_filename
        if not period_type == "hebdo":
            if not full_path.exists():
                target_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(source_file, full_path)
                functions_anfr.log_message(f"Fichier copié vers {full_path}", "INFO")
            else:
                functions_anfr.log_message(f"Fichier déjà présent : {full_path}", "WARN")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pilote l'ensemble des scripts du projet.")
    parser.add_argument('update_type', choices=["hebdo", "mensu", "trim"])
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug pour tous les scripts.")
    args = parser.parse_args()
    main(args)