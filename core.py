#!/usr/bin/env python
import argparse
import subprocess
import sys
import os
import functions_anfr

def run_script(script_name, *args):
    """Exécute un script Python avec des arguments optionnels."""
    try:
        result = subprocess.run([sys.executable, script_name, *args], check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        functions_anfr.log_message(f"Le script {script_name} a échoué avec le code de retour {e.returncode}. Erreur: {e}", "FATAL")
        sys.exit(e.returncode)
    except Exception as e:
        functions_anfr.log_message(f"Une erreur inattendue est survenue lors de l'exécution de {script_name}: {e}", "FATAL")
        sys.exit(1)

def main(args):
    """Fonction principale pour orchestrer l'exécution des différents scripts."""
    # Spécifie les chemins des fichiers
    path_app = os.path.dirname(os.path.abspath(__file__))
    path_compare = os.path.join(path_app, 'compare.py')
    path_pretrait = os.path.join(path_app, 'pretrait.py')
    path_github = os.path.join(path_app, 'github.py')

    if not args.skip_compare:
        functions_anfr.log_message("Exécution de la comparaison des données avec compare.py")
        compare_args = []
        if args.no_file_update:
            compare_args.append('--no-file-update')
        if args.no_download:
            compare_args.append('--no-download')
        if args.no_compare:
            compare_args.append('--no-compare')
        if args.no_write:
            compare_args.append('--no-write')
        if args.debug:
            compare_args.append('--debug')

        run_script(path_compare, *compare_args)

    if not args.skip_pretrait:
        functions_anfr.log_message("Exécution du prétraitement des données avec pretrait.py")
        pretrait_args = []
        if args.no_insee:
            pretrait_args.append('--no-insee')
        if args.no_process:
            pretrait_args.append('--no-process')
        if args.debug:
            pretrait_args.append('--debug')

        run_script(path_pretrait, *pretrait_args)

    if not args.skip_github:
        functions_anfr.log_message("Push vers GitHub avec github.py")
        github_args = []
        # ARGS ARGS ARGS
        github_args.append(args.update_type)
        if args.debug:
            github_args.append('--debug')

        run_script(path_github, *github_args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pilote l'ensemble des scripts du projet.")
    # Arg obligatoire pour déterminer le type de MAJ
    parser.add_argument('update_type', choices=["hebdo", "mensu", "trim"],
                        help="Type de MAJ à effectuer : hebdomadaire, mensuelle ou trimestrielle (pour l'histo, le compare et la manière de lancer github.py)")
    
    # Ajouter des arguments pour skipper certains scripts
    parser.add_argument('--skip-compare', action='store_true', help="Ne pas exécuter le script compare.py.")
    parser.add_argument('--skip-pretrait', action='store_true', help="Ne pas exécuter le script pretrait.py.")
    parser.add_argument('--skip-github', action='store_true', help="Ne pas exécuter le script github.py")
    
    # Ajouter les arguments propres à compare.py
    parser.add_argument('--no-file-update', action='store_true', help="Ne pas mettre à jour les fichiers CSV dans compare.py.")
    parser.add_argument('--no-download', action='store_true', help="Ne pas télécharger les nouvelles données dans compare.py.")
    parser.add_argument('--no-compare', action='store_true', help="Ne pas comparer les données dans compare.py.")
    parser.add_argument('--no-write', action='store_true', help="Ne pas écrire les résultats dans compare.py.")

    # Ajouter les arguments propres à pretrait.py
    parser.add_argument('--no-insee', action='store_true', help="Ne pas charger les données INSEE dans pretrait.py.")
    parser.add_argument('--no-process', action='store_true', help="Ne pas effectuer le traitement des données dans pretrait.py.")

    # Ajouter les arguments propres à github.py
    # ARGS ARGS ARGS
    
    # Argument de débogage global
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug pour tous les scripts.")

    args = parser.parse_args()
    main(args)