#!/usr/bin/env python
from dotenv import load_dotenv
from datetime import datetime
import git
from git import rmtree
import os
import shutil
import git.repo
import argparse
import sys
import functions_anfr

def del_clone_repo(local_dir, repo_url, token):
    """Clone un repo dans un local_dir donné à partir du repo_url, un token doit être donné."""
    if os.path.exists(local_dir):
        rmtree(local_dir)
    repo = git.Repo.clone_from(repo_url.replace("https://", f"https://{token}@"), local_dir)
    return repo

def del_file_repo(local_dir, filename):
    """Supprime un fichier dans un repo téléchargé en local."""
    file_to_del = os.path.join(local_dir, filename)
    if os.path.exists(file_to_del):
        os.remove(file_to_del)
        functions_anfr.log_message(f"Fichier {file_to_del} supprimé.")
    else:
        functions_anfr.log_message(f"Fichier {file_to_del} non trouvé.")

def copy_file_to_repo(local_dir, file_path_in_repo, path_file_to_copy):
    """Copie un fichier dans un repo local."""
    shutil.copy2(path_file_to_copy, os.path.join(local_dir, file_path_in_repo))
    functions_anfr.log_message(f"Nouveau fichier {path_file_to_copy} copié dans le dépôt.")

def commit_modif(repo):
    """ATTENTION !!! Fonction adaptée uniquement au cas présent de la MAJ Hebdo de l'ANFR !!! ATTENTION"""
    now = datetime.now()
    repo.git.add("index.html")
    repo.index.commit(f"Maj du {now.strftime('%d/%m/%Y à %H:%M:%S')}")
    functions_anfr.log_message("Modification commitées.")

def push_to_github(repo):
    """Push le repo vers GitHub."""
    origin = repo.remote(name="origin")
    origin.push()
    functions_anfr.log_message("Modification poussées vers GitHub.")

def main(no_del_clone, no_del_file, no_copy_file, no_commit, no_push, debug):
    """Fonction main régissant l'intégralité du programme."""
    load_dotenv()
    if debug:
        functions_anfr.log_message("Pas de message de debug dans ce programme.")

    # Variables
    path_app = os.path.dirname(os.path.abspath(__file__))
    new_index_file = os.path.join(path_app, "files", "out", "index.html")
    token = os.getenv("GITHUB_TOKEN")
    repo_url = "https://github.com/fraetech/maj-hebdo"
    local_dir = os.path.join(path_app, "maj_hebdo")
    filename = "index.html" # Nom du fichier à supprimer dans le repo

    # 1. Supprimer puis cloner le dépôt (on fait une suppression afin d'éviter les fichiers aux permissions limitées de Git.)
    if not no_del_clone:
        repo = del_clone_repo(local_dir, repo_url, token)
        functions_anfr.log_message("Repo supprimé puis cloné avec succès.")
    else:
        functions_anfr.log_message("La suppression du repo ainsi que son clonage ont été sautés : demandé par argument.", "WARN")
        functions_anfr.log_message("Si cette étape est sautée, le repo n'est pas initialisé. Impossible de continuer.", "FATAL")
        sys.exit(1)

    # 2. Supprimer l'ancien fichier index.html
    if not no_del_file:
        del_file_repo(local_dir, filename)
    else:
        functions_anfr.log_message("Suppression du fichier dans le repo sautée : demandé par argument.", "WARN")

    # 3. Copier le nouveau fichier index.html
    if not no_copy_file:
        copy_file_to_repo(local_dir, filename, new_index_file)
    else:
        
        functions_anfr.log_message("Copie du/des nouveaux fichier(s) dans le repo sautée : demandé par argument. Vous n'allez donc rien 'commiter' ?", "WARN")

    # 4. Commit les modifications
    if not no_commit:
        commit_modif(repo)
    else:
        functions_anfr.log_message("Commit sauté : demandé par argument. Vous n'allez donc rien pousser ?", "WARN")

    # 5. Pousser les modifications vers GitHub
    if not no_push:
        push_to_github(repo)
    else:
        functions_anfr.log_message("Push sauté : demandé par argument.", "WARN")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")

    # Ajouter des arguments pour sauter des étapes
    parser.add_argument('--no-del-clone', action='store_true', help="Ne supprimer le repo stocké localement et le re-télécharger.")
    parser.add_argument('--no-del-file', action='store_true', help="Ne pas supprimer le fichier existant du repo.")
    parser.add_argument('--no-copy-file', action='store_true', help="Ne pas copier le nouveau fichier dans le repo.")
    parser.add_argument('--no-commit', action='store_true', help="Ne pas 'commiter' les modifications.")
    parser.add_argument('--no-push', action='store_true', help="Ne pas pousser les modifications vers GitHub.")
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug.")

    args = parser.parse_args()
    main(no_del_clone=args.no_del_clone, no_del_file=args.no_del_file, no_copy_file=args.no_copy_file, no_commit=args.no_commit, no_push=args.no_push, debug=args.debug)