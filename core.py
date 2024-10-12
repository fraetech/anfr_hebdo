#!/usr/bin/env python
import argparse
import subprocess
import sys
import os
import functions_anfr
import concurrent.futures

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

def run_leaflet_with_operateur(path_leaflet, leaflet_args, operateur=None):
    # Ajouter l'opérateur si précisé
    if operateur:
        leaflet_args_with_operateur = leaflet_args + [f'--operateur={operateur}']
        run_script(path_leaflet, *leaflet_args_with_operateur)
    else:
        run_script(path_leaflet, *leaflet_args)

def main(args):
    """Fonction principale pour orchestrer l'exécution des différents scripts."""
    # Spécifie les chemins des fichiers
    path_app = os.path.dirname(os.path.abspath(__file__))
    path_compare = os.path.join(path_app, 'compare.py')
    path_pretrait = os.path.join(path_app, 'pretrait.py')
    path_leaflet = os.path.join(path_app, 'leaflet.py')
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
    
    if not args.skip_leaflet:
        functions_anfr.log_message("Génération de la carte avec leaflet.py")

        leaflet_args = []
        if args.no_load:
            leaflet_args.append('--no-load')
        if args.no_create_map:
            leaflet_args.append('--no-create-map')
        if args.no_indicators:
            leaflet_args.append('--no-indicators')
        if args.no_save_map:
            leaflet_args.append('--no-save-map')
        if args.debug:
            leaflet_args.append('--debug')

        # Exécution de l'instance sans l'argument --operateur seule
        run_leaflet_with_operateur(path_leaflet, leaflet_args)

        # Liste des opérateurs à exécuter par paire
        operateur_pairs = [
            ["bouygues", "free"],
            ["orange", "sfr"]
        ]

        # Exécution des paires d'opérateurs en parallèle
        for pair in operateur_pairs:
            with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
                # Utilisation de 2 threads pour exécuter les deux opérateurs en parallèle
                futures = [
                    executor.submit(run_leaflet_with_operateur, path_leaflet, leaflet_args, operateur=pair[0]),
                    executor.submit(run_leaflet_with_operateur, path_leaflet, leaflet_args, operateur=pair[1])
                ]

                # Attendre que les deux processus se terminent
                concurrent.futures.wait(futures)

    if not args.skip_github:
        functions_anfr.log_message("Push vers GitHub avec github.py")
        github_args = []
        # ARGS ARGS ARGS
        if args.debug:
            github_args.append('--debug')

        run_script(path_github, *github_args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pilote l'ensemble des scripts du projet.")
    
    # Ajouter des arguments pour skipper certains scripts
    parser.add_argument('--skip-compare', action='store_true', help="Ne pas exécuter le script compare.py.")
    parser.add_argument('--skip-pretrait', action='store_true', help="Ne pas exécuter le script pretrait.py.")
    parser.add_argument('--skip-leaflet', action='store_true', help="Ne pas exécuter le script leaflet.py.")
    parser.add_argument('--skip-github', action='store_true', help="Ne pas exécuter le script github.py")
    
    # Ajouter les arguments propres à compare.py
    parser.add_argument('--no-file-update', action='store_true', help="Ne pas mettre à jour les fichiers CSV dans compare.py.")
    parser.add_argument('--no-download', action='store_true', help="Ne pas télécharger les nouvelles données dans compare.py.")
    parser.add_argument('--no-compare', action='store_true', help="Ne pas comparer les données dans compare.py.")
    parser.add_argument('--no-write', action='store_true', help="Ne pas écrire les résultats dans compare.py.")

    # Ajouter les arguments propres à pretrait.py
    parser.add_argument('--no-insee', action='store_true', help="Ne pas charger les données INSEE dans pretrait.py.")
    parser.add_argument('--no-process', action='store_true', help="Ne pas effectuer le traitement des données dans pretrait.py.")
    
    # Ajouter les arguments propres à leaflet.py
    parser.add_argument('--no-load', action='store_true', help="Ne pas charger les données prétraitées dans leaflet.py.")
    parser.add_argument('--no-create-map', action='store_true', help="Ne pas créer de carte dans leaflet.py.")
    parser.add_argument('--no-indicators', action='store_true', help="Ne pas ajouter de marqueurs sur la carte dans leaflet.py.")
    parser.add_argument('--no-save-map', action='store_true', help="Ne pas sauvegarder la carte dans leaflet.py.")

    # Ajouter les arguments propres à github.py
    # ARGS ARGS ARGS
    
    # Argument de débogage global
    parser.add_argument('--debug', action='store_true', help="Afficher les messages de debug pour tous les scripts.")

    args = parser.parse_args()
    main(args)