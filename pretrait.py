#!/usr/bin/env python
import argparse
import pandas as pd
import os
import csv
import re
import functions_anfr
import numpy as np
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from typing import Dict, Set, Tuple, Optional, List

# Constants optimisés avec frozenset pour des lookups O(1)
ZB_TECHNOS = frozenset({"LTE 700", "LTE 800", "UMTS 900"})
ZB_OPERATEURS = frozenset({"BOUYGUES TELECOM", "FREE MOBILE", "SFR", "ORANGE"})

# Pattern regex pré-compilé pour éviter la recompilation
TECH_PATTERN = re.compile(r'\b((?:GSM|UMTS|LTE))\s(\d{3,4})\b|\b(5G NR)\s(\d{3,5})\b')
TECH_ORDER = {"GSM": 1, "UMTS": 2, "LTE": 3, "5G NR": 4}

# Chargement des chemins une seule fois
fc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "compared", "timestamp.txt")
try:
    with open(fc_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        TIMESTAMP = lines[0].strip()
        OLD_CSV_PATH = lines[1].strip()
        NEW_CSV_PATH = lines[2].strip()
except (FileNotFoundError, IndexError) as e:
    functions_anfr.log_message(f"Erreur lecture timestamp: {e}", "FATAL")
    raise SystemExit(1)

# Dictionnaires de correspondance optimisés
CORRESPONDANCES_TYPE_SUPPORT = {
    0: "Sans nature", 40: "Sémaphore", 41: "Phare", 4: "Château d'eau - réservoir",
    38: "Immeuble", 39: "Local technique", 42: "Mât", 8: "Intérieur galerie",
    9: "Intérieur sous-terrain", 10: "Tunnel", 11: "Mât béton", 12: "Mât métallique",
    21: "Pylône", 17: "Bâtiment", 19: "Monument historique", 20: "Monument religieux",
    22: "Pylône autoportant", 23: "Pylône autostable", 24: "Pylône haubané",
    25: "Pylône treillis", 26: "Pylône tubulaire", 31: "Silo",
    32: "Ouvrage d'art (pont, viaduc)", 33: "Tour hertzienne", 34: "Dalle en béton",
    999999999: "Support non décrit", 43: "Fût", 44: "Tour de contrôle",
    45: "Contre-poids au sol", 46: "Contre-poids sur shelter", 47: "Support DEFENSE",
    48: "Pylône arbre", 49: "Ouvrage de signalisation (portique routier, panneau routier)",
    50: "Balise ou bouée", 51: "XXX", 52: "Éolienne", 55: "Mobilier urbain"
}

CORRESPONDANCES_PROPRIETAIRE_SUPPORT = {
    58: "DAUPHIN TELECOM", 60: "REUNION NUMERIQUE", 45: "RATP",
    46: "Titulaire programme Radio/TV", 47: "Office des Postes et Télécom",
    36: "Altitude Telecom", 37: "Antalis", 38: "One Cast", 40: "Onati",
    41: "France Caraïbes Mobiles", 42: "FREE-MOBILE", 43: "Lagardère Active Média",
    44: "Outremer Telecom", 1: "ANFR", 2: "Association", 3: "Aviation Civile",
    4: "BOUYGUES", 5: "CCI, Ch Métier, Port Aut, Aéroport", 6: "Conseil Départemental",
    7: "Conseil Régional", 8: "Coopérative Agricole, Vinicole", 9: "Copropriété, Syndic, SCI",
    10: "CROSS", 11: "DDE", 13: "EDF ou GDF", 14: "Établissement de soins",
    15: "État, Ministère", 16: "ORANGE Services Fixes", 17: "Syndicat des eaux, Adduction",
    18: "État, Ministère", 19: "La Poste", 20: "Météo", 21: "ORANGE", 22: "Particulier",
    23: "Phares et balises", 24: "SNCF Réseau", 25: "RTE", 26: "SDIS, secours, incendie",
    27: "SFR", 28: "Société HLM", 29: "Société Privée", 30: "Sociétés d'Autoroutes",
    31: "Société Réunionnaise de Radiotéléphonie", 32: "TDF", 33: "Towercast",
    34: "Commune, communauté de communes", 35: "Voies navigables de France",
    39: "État, Ministère", 49: "BOLLORE", 48: "9 CEGETEL", 50: "COMPLETEL",
    51: "DIGICEL", 52: "EUTELSAT", 53: "EXPERTMEDIA", 54: "MEDIASERV", 55: "BELGACOM",
    59: "Itas Tim", 56: "AIRBUS", 57: "GUYANE NUMERIQUE", 62: "SNCF",
    64: "Pacific Mobile Telecom", 63: "VITI", 61: "GLOBECAST", 69: "ZEOP",
    80: "REGIE HTES PYRENEES HAUT DEBIT", 76: "TOWEO", 79: "ONEWEB",
    75: "ON TOWER FRANCE", 68: "CELLNEX", 73: "PHOENIX FRANCE INFRASTRUCTURES",
    81: "VALOCIME", 70: "DGEN", 72: "HIVORY", 78: "NEXT TOWER",
    67: "Service des Postes et Télécom", 65: "ATC FRANCE", 71: "HUBONE",
    74: "TOTEM", 77: "Électricité De Tahiti", 66: "Telco OI", 12: "Autres"
}


class OptimizedProcessor:
    def __init__(self):
        self.insee_data: Dict[str, str] = {}
        self.techs_new_map: Dict[Tuple[str, str], Set[str]] = {}
        self.techs_old_map: Dict[Tuple[str, str], Set[str]] = {}
        self.new_status_dict: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        
        # Cache pour les calculs coûteux
        self._zb_cache: Dict[Tuple[str, str], bool] = {}
        self._new_cache: Dict[Tuple[str, str], bool] = {}
    
    def get_period_code(self, timestamp_str: str, type: str) -> str:
        """Génère le code de période selon le type."""
        dt = datetime.strptime(timestamp_str, "%d/%m/%Y à %H:%M:%S")
        
        if type == "hebdo":
            iso_year, iso_week, _ = dt.isocalendar()
            return f"S{iso_week:02d}_{iso_year}"
        elif type == "mensu":
            return f"{dt.month:02d}_{dt.year}"
        elif type == "trim":
            trimestre = (dt.month - 1) // 3 + 1
            return f"T{trimestre}_{dt.year}"
        else:
            raise ValueError("Type non reconnu. Utiliser 'hebdo', 'mensu' ou 'trim'.")
    
    def load_insee_data_optimized(self, filepath: str, encoding: str = 'utf-8') -> Dict[str, str]:
        """Charge les données INSEE de manière optimisée."""
        try:
            with open(filepath, mode='r', encoding=encoding) as file:
                reader = csv.reader(file, delimiter=';')
                # Création directe du dictionnaire optimisé
                self.insee_data = {
                    row[0].zfill(5): f"{row[2]} {row[1]}"
                    for row in reader if len(row) >= 3 and row[0]
                }
            functions_anfr.log_message("Données INSEE chargées avec succès.")
        except UnicodeDecodeError:
            functions_anfr.log_message(f"Erreur de décodage avec l'encodage {encoding}.", "ERROR")
        except FileNotFoundError:
            functions_anfr.log_message(f"Fichier INSEE '{filepath}' introuvable.", "FATAL")
            raise SystemExit(1)
        except Exception as e:
            functions_anfr.log_message(f"Problème lors du chargement des données INSEE - {e}", "ERROR")
        return self.insee_data
    
    def conv_insee_vectorized(self, codes_insee: pd.Series) -> pd.Series:
        """Version vectorisée de la conversion INSEE."""
        return (codes_insee.astype(str).str.zfill(5)
                .map(self.insee_data)
                .fillna("00404 ERR CONV INSEE"))
    
    def maj_addr_vectorized(self, df: pd.DataFrame) -> pd.Series:
        """Version vectorisée de maj_addr."""
        # Construire l'adresse principale
        addr_cols = ['adresse1', 'adresse2', 'adresse3']
        existing_addr_cols = [col for col in addr_cols if col in df.columns]
        
        if not existing_addr_cols:
            functions_anfr.log_message("Aucune colonne d'adresse trouvée", "WARN")
            return pd.Series(["00404 ERR ADDRESS"] * len(df), index=df.index)
        
        addr_parts = df[existing_addr_cols].fillna('').apply(
            lambda row: ' '.join(filter(None, row.astype(str))), axis=1
        )
        
        # Ajouter adresse0 si présente
        if 'adresse0' in df.columns:
            mask = df['adresse0'].notna()
            if mask.any():
                addr_parts.loc[mask] += ' (' + df.loc[mask, 'adresse0'].astype(str) + ')'
        
        # Conversion INSEE vectorisée
        if 'code_insee' in df.columns:
            insee_converted = self.conv_insee_vectorized(df['code_insee'])
            return (addr_parts + ' ' + insee_converted).str.upper()
        else:
            functions_anfr.log_message("Colonne code_insee manquante", "WARN")
            return (addr_parts + ' 00404 ERR CONV INSEE').str.upper()
    
    def preprocess_csv_optimized(self, file_path: str, source: str) -> pd.DataFrame:
        """Version optimisée du chargement CSV."""
        try:
            # Colonnes de base toujours présentes
            base_cols = [
                'id_support', 'operateur', 'technologie', 'statut_x', 
                'adresse0', 'adresse1', 'adresse2', 'adresse3', 
                'code_insee', 'coordonnees', 'date_activ_x'
            ]
            
            # Colonnes avec suffixes possibles
            suffixed_cols = [
                'type_support_x', 'type_support_y',
                'hauteur_support_x', 'hauteur_support_y', 
                'proprietaire_support_x', 'proprietaire_support_y',
                'statut_y', 'date_activ_y'
            ]
            
            # Charger d'abord avec toutes les colonnes pour voir ce qui existe
            df_sample = pd.read_csv(file_path, nrows=0)
            available_cols = df_sample.columns.tolist()
            
            # Prendre les colonnes de base disponibles + les colonnes avec suffixe disponibles
            usecols = [col for col in base_cols + suffixed_cols if col in available_cols]
            
            # Chargement avec les colonnes disponibles seulement
            df = pd.read_csv(file_path, usecols=usecols, dtype=str, na_filter=True)
            df['source'] = source
            
            functions_anfr.log_message(f"Chargement du fichier '{file_path}' terminé avec succès.")
            functions_anfr.log_message(f"Colonnes chargées: {usecols}")
            
            return df
            
        except Exception as e:
            functions_anfr.log_message(f"Erreur chargement '{file_path}' - {e}", "FATAL")
            raise SystemExit(1)
    
    @lru_cache(maxsize=1000)
    def sort_technologies_optimized(self, tech_string: str) -> str:
        """Version optimisée et mise en cache du tri des technologies."""
        if not tech_string or pd.isna(tech_string):
            return ""
        
        elements = tech_string.split(", ")
        
        def sort_key(tech: str) -> Tuple[int, int]:
            match = TECH_PATTERN.match(tech.strip())
            if match:
                technology = match.group(1) or match.group(3)
                frequency = match.group(2) or match.group(4)
                return (TECH_ORDER.get(technology, float('inf')), int(frequency))
            return (float('inf'), 0)
        
        return ", ".join(sorted(elements, key=sort_key))
    
    def determine_action_vectorized(self, df: pd.DataFrame) -> pd.Series:
        """Version vectorisée de determine_action."""
        result = pd.Series(index=df.index, dtype=str)
        
        # AJO ou ALL pour comp_added
        mask_ajo = df['source'] == 'comp_added.csv'

        # Cas spécifiques : si statut_y est "En service" ou "Techniquement opérationnel"
        mask_act = mask_ajo & df['statut_y'].isin(["En service", "Techniquement opérationnel"])

        result.loc[mask_act] = 'ALL'
        result.loc[mask_ajo & ~mask_act] = 'AJO'

        
        # SUP pour comp_removed
        mask_sup = df['source'] == 'comp_removed.csv'
        result.loc[mask_sup] = 'SUP'
        
        # Logique complexe pour comp_modified
        mask_mod = df['source'] == 'comp_modified.csv'
        if mask_mod.any():
            mod_df = df.loc[mask_mod].copy()
            
            # Vérifier la présence des colonnes nécessaires
            required_cols = ['statut_x', 'statut_y', 'date_activ_x', 'date_activ_y']
            missing_cols = [col for col in required_cols if col not in mod_df.columns]
            
            if missing_cols:
                functions_anfr.log_message(f"Colonnes manquantes pour determine_action: {missing_cols}", "WARN")
                result.loc[mask_mod] = "UNKNOWN"
                return result.fillna("UNKNOWN")
            
            # Normalisation des dates avec gestion sécurisée
            for col in ['date_activ_x', 'date_activ_y']:
                mod_df[col] = mod_df[col].fillna('').astype(str).str.strip()
                mod_df[col] = mod_df[col].replace('', None)
            
            # Remplacer les valeurs NaN par des chaînes vides pour éviter les erreurs .isin()
            statut_x = mod_df['statut_x'].fillna('')
            statut_y = mod_df['statut_y'].fillna('')
            date_activ_x = mod_df['date_activ_x'].fillna('')
            date_activ_y = mod_df['date_activ_y'].fillna('')
            
            # Conditions vectorisées avec gestion sécurisée des NaN
            cond_aav = (
                (statut_x == 'Projet approuvé') & 
                (statut_y == 'Projet approuvé') &
                (date_activ_x != date_activ_y)
            )
            
            cond_all = (
                (statut_x == 'Projet approuvé') & 
                (statut_y.isin(['Techniquement opérationnel', 'En service']))
            )
            
            cond_ext = (
                (statut_x.isin(['En service', 'Techniquement opérationnel'])) &
                (statut_y == 'Projet approuvé')
            )
            
            # Appliquer les conditions avec les index corrects
            mod_indices = mod_df.index
            result.loc[mod_indices[cond_aav]] = 'AAV'
            result.loc[mod_indices[cond_all & ~cond_aav]] = 'ALL'
            result.loc[mod_indices[cond_ext & ~cond_aav & ~cond_all]] = 'EXT'
        
        return result.fillna("UNKNOWN")
    
    def extract_tech_dict_optimized(self, df: pd.DataFrame) -> Dict[Tuple[str, str], Set[str]]:
        """Version optimisée d'extract_tech_dict."""
        df_clean = df.dropna(subset=["sup_id", "adm_lb_nom", "emr_lb_systeme"])
        if df_clean.empty:
            return {}
        
        # Groupby optimisé avec transformation directe
        grouped = (df_clean.groupby(["sup_id", "adm_lb_nom"])["emr_lb_systeme"]
                  .apply(lambda x: frozenset(x.unique()))
                  .to_dict())
        
        return {k: set(v) for k, v in grouped.items()}
    
    def build_new_status_map_optimized(self, df_old: pd.DataFrame) -> Dict[Tuple[str, str], List[str]]:
        """Version optimisée de build_new_status_map."""
        required_cols = ["sup_id", "adm_lb_nom", "statut"]
        missing_cols = [col for col in required_cols if col not in df_old.columns]
        
        if missing_cols:
            functions_anfr.log_message(f"Colonnes manquantes pour build_new_status_map: {missing_cols}", "WARN")
            return defaultdict(list)
        
        df_clean = df_old.dropna(subset=required_cols)
        if df_clean.empty:
            return defaultdict(list)
        
        grouped = df_clean.groupby(["sup_id", "adm_lb_nom"])["statut"].apply(list)
        return defaultdict(list, grouped.to_dict())
    
    def is_zb_cached(self, support_id: str, operateur: str) -> bool:
        """Version mise en cache de is_zb."""
        if operateur not in ZB_OPERATEURS:
            return False
            
        key = (str(support_id).strip(), operateur)
        
        if key in self._zb_cache:
            return self._zb_cache[key]
        
        techs_new = self.techs_new_map.get(key, set())
        techs_old = self.techs_old_map.get(key, set())
        
        result = (
            (techs_new and techs_new <= ZB_TECHNOS) or
            (techs_old and techs_old <= ZB_TECHNOS)
        )
        
        self._zb_cache[key] = result
        return result
    
    def is_new_cached(self, support_id: str, operateur: str) -> bool:
        """Version mise en cache de is_new."""
        key = (str(support_id).strip(), operateur)
        
        if key in self._new_cache:
            return self._new_cache[key]
        
        statuts = self.new_status_dict.get(key, [])
        result = not statuts or all(stat == "Projet approuvé" for stat in statuts)
        
        self._new_cache[key] = result
        return result
    
    def find_and_isolate_duplicates_optimized(self, df: pd.DataFrame, 
                                            location_threshold: float = 0.001, 
                                            address_similarity_threshold: float = 0.5) -> pd.DataFrame:
        """Version optimisée de la détection de doublons avec pré-filtrage spatial."""
        if len(df) < 2:
            return pd.DataFrame()
        
        # Pré-filtrage par grille spatiale
        coords = df['coordonnees'].str.split(', ', expand=True).astype(float)
        grid_size = location_threshold * 5  # Grille plus large pour capturer les voisins
        
        df_work = df.copy()
        df_work['grid_x'] = (coords.iloc[:, 0] / grid_size).astype(int)
        df_work['grid_y'] = (coords.iloc[:, 1] / grid_size).astype(int)
        
        # Ajouter les grilles voisines pour éviter les effets de bord
        neighbors = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 0), (0, 1), (1, -1), (1, 0), (1, 1)]
        
        duplicates_list = []
        processed_pairs = set()
        
        for _, row in df_work.iterrows():
            if row.name in processed_pairs:
                continue
            
            # Chercher dans les grilles voisines
            candidates = []
            for dx, dy in neighbors:
                grid_x, grid_y = row['grid_x'] + dx, row['grid_y'] + dy
                mask = (df_work['grid_x'] == grid_x) & (df_work['grid_y'] == grid_y)
                candidates.extend(df_work[mask].index.tolist())
            
            candidates = list(set(candidates))  # Remove duplicates
            candidates = [c for c in candidates if c != row.name and c not in processed_pairs]
            
            if not candidates:
                continue
            
            candidate_rows = df_work.loc[candidates]
            
            # Vérifications vectorisées sur les candidats
            coords_row = np.array([float(x) for x in row['coordonnees'].split(', ')])
            coords_candidates = np.array([
                [float(x) for x in coord.split(', ')] 
                for coord in candidate_rows['coordonnees']
            ])
            
            # Distance euclidienne vectorisée
            distances = np.sqrt(np.sum((coords_candidates - coords_row) ** 2, axis=1))
            location_matches = distances <= location_threshold
            
            if not location_matches.any():
                continue
            
            # Vérifier la similarité d'adresse et les autres critères
            for i, candidate_idx in enumerate(candidates):
                if not location_matches[i]:
                    continue
                
                candidate = df_work.loc[candidate_idx]
                
                # Similarité d'adresse simple
                addr1_tokens = set(row['adresse'].lower().split())
                addr2_tokens = set(candidate['adresse'].lower().split())
                intersection = addr1_tokens & addr2_tokens
                union = addr1_tokens | addr2_tokens
                addr_similarity = len(intersection) / len(union) if union else 0
                
                if addr_similarity < address_similarity_threshold:
                    continue
                
                # Vérifier technologie, opérateur et action
                tech1 = frozenset(row['technologie'].split(", "))
                tech2 = frozenset(candidate['technologie'].split(", "))
                
                if (tech1 == tech2 and 
                    row['operateur'] == candidate['operateur'] and 
                    row['action'] != candidate['action']):
                    
                    duplicates_list.extend([row.name, candidate_idx])
                    processed_pairs.update([row.name, candidate_idx])
        
        if duplicates_list:
            return df.loc[list(set(duplicates_list))]
        return pd.DataFrame()
    
    def merge_and_process_optimized(self, added_path: str, modified_path: str, 
                                  removed_path: str, output_path: str) -> None:
        """Version optimisée de merge_and_process."""
        try:
            # Chargement optimisé des fichiers
            added_df = self.preprocess_csv_optimized(added_path, 'comp_added.csv')
            modified_df = self.preprocess_csv_optimized(modified_path, 'comp_modified.csv')
            removed_df = self.preprocess_csv_optimized(removed_path, 'comp_removed.csv')
            
            if added_df.empty and modified_df.empty and removed_df.empty:
                functions_anfr.log_message("Tous les fichiers sont vides.", "FATAL")
                raise SystemExit(1)
            
            # Détermination des actions de manière vectorisée
            all_dfs = [added_df, modified_df, removed_df]
            for df in all_dfs:
                if not df.empty:
                    df['action'] = self.determine_action_vectorized(df)
            
            # Concaténation
            final_df = pd.concat([df for df in all_dfs if not df.empty], ignore_index=True)
            
            # Uniformisation des colonnes avec combine_first vectorisé
            # Gérer les colonnes avec suffixes _x et _y
            combine_cols = {
                'type_support': ['type_support_x', 'type_support_y'],
                'hauteur_support': ['hauteur_support_x', 'hauteur_support_y'],
                'proprietaire_support': ['proprietaire_support_x', 'proprietaire_support_y'],
                'date_activ': ['date_activ_x', 'date_activ_y'],
                'statut': ['statut_x', 'statut_y']  # Ajout de statut aussi
            }
            
            for target, sources in combine_cols.items():
                if all(col in final_df.columns for col in sources):
                    final_df[target] = final_df[sources[0]].combine_first(final_df[sources[1]])
                elif sources[0] in final_df.columns:
                    final_df[target] = final_df[sources[0]]
                elif sources[1] in final_df.columns:
                    final_df[target] = final_df[sources[1]]
                else:
                    # Si aucune colonne n'existe, créer une colonne vide
                    final_df[target] = None
            
            # Mise à jour des adresses vectorisée
            final_df['adresse'] = self.maj_addr_vectorized(final_df)
            
            # Agrégation optimisée
            agg_dict = {
                'technologie': lambda x: ', '.join(x),
                'adresse': 'first',
                'code_insee': 'first',
                'coordonnees': 'first',
                'type_support': 'first',
                'hauteur_support': 'first',
                'proprietaire_support': 'first',
                'date_activ': 'first'
            }
            
            final_df = (final_df.groupby(['id_support', 'operateur', 'action'], as_index=False)
                       .agg(agg_dict))
            
            # Tri des technologies avec cache
            final_df['technologie'] = final_df['technologie'].apply(self.sort_technologies_optimized)
            
            # Transformations des supports avec map vectorisé
            final_df['type_support'] = (final_df['type_support'].fillna("Inconnu")
                                       .astype(str)
                                       .replace('nan', 'Inconnu')
                                       .apply(lambda x: int(float(x)) if x.replace('.','').isdigit() else x)
                                       .map(CORRESPONDANCES_TYPE_SUPPORT)
                                       .fillna("Inconnu"))
            
            final_df['proprietaire_support'] = (final_df['proprietaire_support'].fillna("Inconnu")
                                               .astype(str)
                                               .replace('nan', 'Inconnu')
                                               .apply(lambda x: int(float(x)) if x.replace('.','').isdigit() else x)
                                               .map(CORRESPONDANCES_PROPRIETAIRE_SUPPORT)
                                               .fillna("Inconnu"))
            
            final_df['hauteur_support'] = (final_df['hauteur_support'].fillna(0)
                                          .astype(str)
                                          .apply(lambda x: f"{x.replace('.', ',')}m"))
            
            # Tri et suppression des doublons
            final_df = final_df.sort_values(['id_support', 'operateur', 'action']).reset_index(drop=True)
            
            # Suppression des doublons stricts
            duplicated = final_df.duplicated(subset=['id_support', 'operateur', 'technologie'], keep=False)
            final_df = final_df[~duplicated]
            
            # Détection des doublons complexes
            duplicates_df = self.find_and_isolate_duplicates_optimized(final_df)
            if not duplicates_df.empty:
                final_df = final_df[~final_df.index.isin(duplicates_df.index)]
            
            # Arrondi des coordonnées vectorisé
            coords_split = final_df['coordonnees'].str.split(',', expand=True).astype(float)
            final_df['coordonnees'] = (coords_split.round(4).astype(str)
                                     .apply(lambda x: ','.join(x), axis=1))
            
            # Calcul optimisé des flags is_zb et is_new
            final_df['technologie_set'] = final_df['technologie'].apply(lambda x: frozenset(x.split(', ')))
            
            # Calculs par batch pour is_zb
            unique_pairs = final_df[['id_support', 'operateur']].drop_duplicates()
            zb_results = {}
            new_results = {}
            
            for _, row in unique_pairs.iterrows():
                key = (row['id_support'], row['operateur'])
                zb_results[key] = self.is_zb_cached(row['id_support'], row['operateur'])
                new_results[key] = self.is_new_cached(row['id_support'], row['operateur'])
            
            # Application vectorisée des résultats
            final_df['is_zb'] = final_df.apply(lambda x: zb_results.get((x['id_support'], x['operateur']), False), axis=1)
            final_df['is_new'] = final_df.apply(lambda x: new_results.get((x['id_support'], x['operateur']), False), axis=1)
            
            # Nettoyage final
            final_df = final_df.drop('technologie_set', axis=1)
            
            # Génération des fichiers par opérateur avec des filtres vectorisés
            operator_mapping = {
                'bouygues.csv': final_df['operateur'] == "BOUYGUES TELECOM",
                'free.csv': final_df['operateur'].isin(["FREE MOBILE", "TELCO OI"]),
                'orange.csv': final_df['operateur'] == "ORANGE",
                'sfr.csv': final_df['operateur'].isin(["SFR", "SRR"])
            }
            
            # Sauvegarde optimisée
            final_df.to_csv(os.path.join(output_path, 'index.csv'), index=False)
            
            for filename, mask in operator_mapping.items():
                operator_df = final_df[mask]
                if not operator_df.empty:
                    operator_df.to_csv(os.path.join(output_path, filename), index=False)
            
            # Fichier avec timestamp
            time_period = self.get_period_code(TIMESTAMP, args.update_type)
            final_df.to_csv(os.path.join(output_path, f"{time_period}.csv"), index=False)
            
            functions_anfr.log_message("Fichiers finaux générés avec succès, duplications supprimées.")
            
        except Exception as e:
            functions_anfr.log_message(f"Échec lors du traitement des fichiers - {e}", "FATAL")
            raise SystemExit(1)


def main(no_insee, no_process, debug):
    """Fonction principale optimisée."""
    processor = OptimizedProcessor()
    
    path_app = os.path.dirname(os.path.abspath(__file__))
    added_path = os.path.join(path_app, 'files', 'compared', 'comp_added.csv')
    modified_path = os.path.join(path_app, 'files', 'compared', 'comp_modified.csv')
    removed_path = os.path.join(path_app, 'files', 'compared', 'comp_removed.csv')
    insee_path = os.path.join(path_app, 'files', 'cc_insee', 'cc_insee.csv')
    pretraite_path = os.path.join(path_app, 'files', 'pretraite')

    # Chargement des données une seule fois au début
    functions_anfr.log_message("Début du chargement des fichiers CSV principaux...", "INFO")
    try:
        # Chargement complet pour extract_tech_dict et build_new_status_map
        functions_anfr.log_message(f"Chargement de {os.path.basename(OLD_CSV_PATH)}...", "INFO")
        df_old = pd.read_csv(OLD_CSV_PATH, sep=",", on_bad_lines="skip", dtype=str)
        functions_anfr.log_message(f"✓ {os.path.basename(OLD_CSV_PATH)} chargé ({len(df_old):,} lignes)", "INFO")
        
        functions_anfr.log_message(f"Chargement de {os.path.basename(NEW_CSV_PATH)}...", "INFO")
        df_new = pd.read_csv(NEW_CSV_PATH, sep=",", on_bad_lines="skip", dtype=str)
        functions_anfr.log_message(f"✓ {os.path.basename(NEW_CSV_PATH)} chargé ({len(df_new):,} lignes)", "INFO")
        
        # Vérifier les colonnes nécessaires pour tech extraction
        required_tech_cols = ["sup_id", "adm_lb_nom", "emr_lb_systeme"]
        old_has_tech_cols = all(col in df_old.columns for col in required_tech_cols)
        new_has_tech_cols = all(col in df_new.columns for col in required_tech_cols)
        
        if not old_has_tech_cols:
            functions_anfr.log_message(f"Colonnes tech manquantes dans OLD_CSV: {[col for col in required_tech_cols if col not in df_old.columns]}", "WARN")
        if not new_has_tech_cols:
            functions_anfr.log_message(f"Colonnes tech manquantes dans NEW_CSV: {[col for col in required_tech_cols if col not in df_new.columns]}", "WARN")
            
    except Exception as e:
        functions_anfr.log_message(f"Erreur lors de la lecture du CSV : {e}", "ERROR")
        raise

    # Préparation des données tech et status une seule fois
    functions_anfr.log_message("Préparation des index technologie et statuts...", "INFO")
    
    if new_has_tech_cols:
        processor.techs_new_map = processor.extract_tech_dict_optimized(df_new)
        functions_anfr.log_message(f"✓ Index tech NEW créé ({len(processor.techs_new_map):,} entrées)", "INFO")
    else:
        processor.techs_new_map = {}
        
    if old_has_tech_cols:
        processor.techs_old_map = processor.extract_tech_dict_optimized(df_old)
        functions_anfr.log_message(f"✓ Index tech OLD créé ({len(processor.techs_old_map):,} entrées)", "INFO")
        
        # Pour new_status_dict, vérifier aussi la présence de 'statut'
        if "statut" in df_old.columns:
            processor.new_status_dict = processor.build_new_status_map_optimized(df_old)
            functions_anfr.log_message(f"✓ Index statuts créé ({len(processor.new_status_dict):,} entrées)", "INFO")
        else:
            functions_anfr.log_message("Colonne 'statut' manquante dans OLD_CSV pour is_new", "WARN")
            processor.new_status_dict = defaultdict(list)
    else:
        processor.techs_old_map = {}
        processor.new_status_dict = defaultdict(list)

    # Chargement INSEE optimisé
    if not no_insee:
        processor.load_insee_data_optimized(insee_path, encoding='ISO-8859-1')
    else:
        functions_anfr.log_message("Chargement INSEE sauté : demandé par argument", "WARN")

    # Traitement principal
    if not no_process:
        processor.merge_and_process_optimized(added_path, modified_path, removed_path, pretraite_path)
        functions_anfr.log_message("Prétraitement terminé")
    else:
        functions_anfr.log_message("Prétraitement sauté : demandé par argument", "WARN")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Control which functions to skip.")

    # Arguments identiques à l'original
    parser.add_argument('update_type', choices=["hebdo", "mensu", "trim"])
    parser.add_argument('--no-insee', action='store_true', 
                       help="Ne pas charger les données INSEE, ne pas modifier les adresses.")
    parser.add_argument('--no-process', action='store_true', 
                       help="Ne pas effectuer le traitement des données.")
    parser.add_argument('--debug', action='store_true', 
                       help="Afficher les messages de debug.")
    
    args = parser.parse_args()

    main(no_insee=args.no_insee, no_process=args.no_process, debug=args.debug)