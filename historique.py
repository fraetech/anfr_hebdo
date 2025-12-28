#!/usr/bin/env python
import argparse
import os
import csv
import locale
import shutil
import functions_anfr
from datetime import datetime, timedelta
from pathlib import Path

locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')

fc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "compared", "timestamp.txt")
with open(fc_file, "r", encoding="utf-8") as f:
    lines = f.readlines()
    TIMESTAMP = lines[0].strip()


def get_actual_week_for_data(timestamp_str: str) -> tuple:
    """Détermine la vraie semaine ISO des données basée sur le jour de publication.
    
    Args:
        timestamp_str: Timestamp au format "%d/%m/%Y à %H:%M:%S"
    
    Returns:
        Tuple (iso_week, iso_year) représentant la semaine des données réelles
    
    Logique:
        - Jeudi/Vendredi (4,5) : données de la semaine courante N
        - Lundi/Mardi/Mercredi (1,2,3) : données de la semaine précédente N-1 (rattrapage)
    """
    dt = datetime.strptime(timestamp_str, "%d/%m/%Y à %H:%M:%S")
    iso_year, iso_week, iso_day = dt.isocalendar()
    
    # Si lundi, mardi ou mercredi (jours 1, 2, 3), c'est un rattrapage de la semaine précédente
    if iso_day in [1, 2, 3]:
        # Retourner la semaine précédente
        previous_week_date = dt - timedelta(days=7)
        iso_year, iso_week, _ = previous_week_date.isocalendar()
    
    return iso_week, iso_year


def build_label_and_path(period_code: str, dt: datetime, type_: str):
    """Construit le label et le chemin pour une entrée d'historique.
    
    Pour les hebdo, utilise la vraie semaine déduite du jour de publication.
    """
    if type_ == "hebdo":
        iso_year, iso_week, iso_day = dt.isocalendar()
        
        # Calculer le lundi de la semaine ISO
        monday = dt - timedelta(days=iso_day - 1)
        # Calculer le dimanche de la semaine ISO
        sunday = monday + timedelta(days=6)
        
        # Formater les dates au format JJ/MM
        date_debut = monday.strftime("%d/%m")
        date_fin = sunday.strftime("%d/%m")
        label = f"{date_debut} - {date_fin} {iso_year} (S{iso_week:02d})"
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
    """Met à jour l'historique en gérant intelligemment les doublons hebdomadaires.
    
    Pour les MAJ hebdomadaires:
    - Si publiée jeudi/vendredi: correspond à la semaine courante
    - Si publiée lundi/mardi/mercredi: correspond à la semaine précédente (rattrapage)
    - Si plusieurs MAJ pour la même semaine: on remplace l'ancienne par la nouvelle
    """
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
    
    if type_ == "hebdo":
        # Déduire la vraie semaine basée sur le jour de publication
        actual_week, actual_year = get_actual_week_for_data(timestamp_str)
        actual_period = f"S{actual_week:02d}_{actual_year}"
        
        # Chercher si une entrée existe déjà pour cette vraie semaine
        # (peu importe le path exact, on cherche la base "hebdo/S##_YYYY")
        existing_entry = None
        entry_index = None
        for idx, row in enumerate(existing_rows):
            if row["type"] == "hebdo":
                # Extraire la base du path (sans suffixe -v#)
                row_base_path = row["path"].split("-v")[0]  # "hebdo/S##_YYYY"
                # Comparer les codes de semaine
                if actual_period in row_base_path:
                    existing_entry = row
                    entry_index = idx
                    break
        
        if existing_entry:
            # Remplacer l'entrée existante (plus récente remplace l'ancienne)
            existing_rows[entry_index] = {
                "type": type_,
                "label": label,
                "path": path
            }
            functions_anfr.log_message(f"MAJ existante remplacée : {type_},{label},{path}")
        else:
            # Ajouter une nouvelle entrée
            existing_rows.append({
                "type": type_,
                "label": label,
                "path": path
            })
            functions_anfr.log_message(f"Ajouté : {type_},{label},{path}")
    else:
        # Pour mensu et trim, logique simple : vérifier l'existence exacte
        existing_entry = None
        entry_index = None
        for idx, row in enumerate(existing_rows):
            if row["type"] == type_ and row["path"] == path:
                existing_entry = row
                entry_index = idx
                break
        
        if existing_entry:
            functions_anfr.log_message("Entrée déjà présente, aucune modification.", "INFO")
            return
        else:
            existing_rows.append({
                "type": type_,
                "label": label,
                "path": path
            })
            functions_anfr.log_message(f"Ajouté : {type_},{label},{path}")
    
    # Réécrire le fichier CSV avec les lignes mises à jour
    with open(history_path, mode="w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["type", "label", "path"])
        writer.writeheader()
        writer.writerows(existing_rows)

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