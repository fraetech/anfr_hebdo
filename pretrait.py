#!/usr/bin/env python
import argparse
import pandas as pd
import os
import csv
import re
import functions_anfr
import numpy as np
import math
from collections import defaultdict
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, Set, Tuple, Optional, List

# Constants optimisés avec frozenset pour des lookups O(1)
ZB_TECHNOS = frozenset({"LTE 700", "LTE 800", "UMTS 900", "LTE 1800"})
ZB_OPERATEURS = frozenset({"BOUYGUES TELECOM", "FREE MOBILE", "SFR", "ORANGE"})
ZB_CORE_TECHNOS = frozenset({"UMTS 900", "LTE 700", "LTE 800"})

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
        DATA_DATE = lines[3].strip() if len(lines) > 3 else ""
except (FileNotFoundError, IndexError) as e:
    functions_anfr.log_message(f"Erreur lecture timestamp: {e}", "FATAL")
    raise SystemExit(1)

ACTIVATION_GRACE_DAYS = {
    "hebdo": 28,
    "mensu": 56,
    "trim": 112,
}
ACTIVATION_LIMIT_DATE = (datetime.strptime(TIMESTAMP, "%d/%m/%Y à %H:%M:%S") - timedelta(days=28)).strftime("%Y-%m-%d")

# Dictionnaires de correspondance optimisés
# Dictionnaires de correspondance optimisés
CORRESPONDANCES_TYPE_SUPPORT = {
    0: "Sans nature", 40: "Sémaphore", 41: "Phare",
    4: "Château d'eau - réservoir", 38: "Immeuble",
    39: "Local technique", 42: "Mât", 8: "Intérieur galerie",
    9: "Intérieur sous-terrain", 10: "Tunnel", 11: "Mât béton",
    12: "Mât métallique", 21: "Pylône", 17: "Bâtiment",
    19: "Monument historique", 20: "Monument religieux",
    22: "Pylône autoportant", 23: "Pylône autostable",
    24: "Pylône haubané", 25: "Pylône treillis",
    26: "Pylône tubulaire", 31: "Silo",
    32: "Ouvrage d'art (pont, viaduc)", 33: "Tour hertzienne",
    34: "Dalle en béton", 999999999: "Support non décrit",
    43: "Fût", 44: "Tour de contrôle",
    45: "Contre-poids au sol", 46: "Contre-poids sur shelter",
    47: "Support DEFENSE", 48: "Pylône arbre",
    49: "Ouvrage de signalisation (portique routier, panneau routier)",
    50: "Balise ou bouée", 51: "XXX",
    52: "Éolienne", 55: "Mobilier urbain"
}

CORRESPONDANCES_PROPRIETAIRE_SUPPORT = {
    1: "ANFR", 2: "Association", 3: "Aviation Civile", 4: "BOUYGUES",
    5: "CCI, Ch Métiers, Port Aut, Aéroport", 6: "Conseil Départemental",
    7: "Conseil Régional", 8: "Coopérative Agricole, Vinicole",
    9: "Copropriété, Syndic, SCI", 10: "CROSS", 11: "DDE", 12: "Autres",
    13: "EDF ou GDF", 14: "Établissement de soins",
    15: "Ets public, Minist, Synd mixt", 16: "ORANGE Services Fixes",
    17: "Syndicat des eaux, Adduction", 18: "État Ministère",
    19: "La Poste", 20: "Météo", 21: "ORANGE", 22: "Particulier",
    23: "Phares et balises", 24: "SNCF Réseau", 25: "RTE",
    26: "SDIS, secours, incendie", 27: "SFR", 28: "Société HLM",
    29: "Société Privée", 30: "Sociétés d'Autoroutes",
    31: "Sté Réunionn. de Radiotéléph.", 32: "TDF", 33: "Towercast",
    34: "Commune, communauté de commune", 35: "Voies navigables de France",
    36: "Altitude Telecom", 37: "Antalis", 38: "One Cast",
    39: "État Ministère", 40: "Onati", 41: "France Caraïbes Mobiles",
    42: "FREE-MOBILE", 43: "Lagardère Active Média",
    44: "Outremer Telecom", 45: "RATP",
    46: "Titulaire programme Radio/TV",
    47: "Office des Postes et Telecom", 48: "9 CEGETEL",
    49: "BOLLORE", 50: "COMPLETEL", 51: "DIGICEL", 52: "EUTELSAT",
    53: "EXPERTMEDIA", 54: "MEDIASERV", 55: "BELGACOM", 56: "AIRBUS",
    57: "GUYANE NUMERIQUE", 58: "DAUPHIN TELECOM", 59: "Itas Tim",
    60: "REUNION NUMERIQUE", 61: "GLOBECAST", 62: "SNCF", 63: "VITI",
    64: "Pacific Mobile Telecom", 65: "ATC FRANCE", 66: "Telco OI",
    67: "Service des Postes et Telecom", 68: "CELLNEX", 69: "ZEOP",
    70: "DGEN", 71: "HUBONE", 72: "HIVORY",
    73: "PHOENIX FRANCE INFRASTRUCTURES", 74: "TOTEM",
    75: "ON TOWER FRANCE", 76: "TOWEO",
    77: "Électricité De Tahiti", 78: "NEXT TOWER",
    79: "ONEWEB", 80: "REGIE HTES PYRENEES HAUT DEBIT",
    81: "VALOCIME", 82: "SYADEN", 85: "Société des Grands Projets"
}


class OptimizedProcessor:
    def __init__(self, update_type: str = "hebdo"):
        if update_type not in ACTIVATION_GRACE_DAYS:
            raise ValueError(f"Type de mise à jour inconnu : {update_type}")
        self.insee_data: Dict[str, str] = {}
        self.activation_limit_date = (
            datetime.strptime(TIMESTAMP, "%d/%m/%Y à %H:%M:%S") -
            timedelta(days=ACTIVATION_GRACE_DAYS[update_type])
        ).strftime("%Y-%m-%d")
        self.techs_new_map: Dict[Tuple[str, str], Set[str]] = {}
        self.techs_old_map: Dict[Tuple[str, str], Set[str]] = {}
        self.new_status_dict: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        self.new_azimuth_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        self.zb_site_map: Dict[str, bool] = {}
        
        # Cache pour les calculs coûteux
        self._zb_cache: Dict[Tuple[str, str], bool] = {}
        self._new_cache: Dict[Tuple[str, str], bool] = {}
    
    def load_insee_data_optimized(self, filepath: str, encoding: str = 'utf-8') -> Dict[str, str]:
        """Charge les données INSEE de manière optimisée."""
        try:
            self.insee_data = {}
            with open(filepath, mode='r', encoding=encoding, newline='') as file:
                reader = csv.reader(file, delimiter=';')
                for row in reader:
                    if len(row) < 3:
                        continue
                    code = str(row[0]).strip().replace('\ufeff', '')
                    if not code or code.startswith('#Code_commune_INSEE') or not code.isdigit():
                        continue
                    city = str(row[1]).strip()
                    postal = str(row[2]).strip()
                    if not city or not postal:
                        continue
                    self.insee_data[code.zfill(5)] = f"{postal} {city}"
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
        """Version vectorisée de la conversion INSEE.

        Si un code commune n'est pas présent dans la table INSEE, on garde le code brut
        au lieu d'injecter le faux libellé 00404 ERR CONV INSEE qui pollue tout le front.
        """
        normalized = (codes_insee.astype(str)
                      .str.strip()
                      .str.replace(r'\D', '', regex=True)
                      .str.zfill(5))

        if not self.insee_data:
            return normalized

        mapped = normalized.map(self.insee_data)
        return mapped.fillna(normalized)
    
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
    
    def preprocess_csv_optimized(self, file_path: str, source: str, sep: str) -> pd.DataFrame:
        """Version optimisée du chargement CSV."""
        
        try:
            # Colonnes de base toujours présentes
            base_cols = [
                'id_support', 'operateur', 'technologie', 'statut_x', 
                'adresse0', 'adresse1', 'adresse2', 'adresse3', 
                'code_insee', 'coordonnees', 'date_activ_x',
                'type_support', 'hauteur_support', 'proprietaire_support'
            ]
            
            # Colonnes avec suffixes possibles
            suffixed_cols = [
                'statut_y', 'date_activ_y', 'list_azimut_last',
                'list_azimut_old', 'action', 'azimuth_scope',
                'azimuth_changed_technologies'
            ]
            base_cols.append('list_azimut')
            
            # Prendre les colonnes de base disponibles + les colonnes avec suffixe disponibles
            df = pd.read_csv(file_path, on_bad_lines="skip", dtype=str, sep=sep, engine='c')
            df['source'] = source
            usecols = [col for col in base_cols + suffixed_cols if col in df.columns]
            
            # Chargement avec les colonnes disponibles seulement
            df = pd.read_csv(file_path, on_bad_lines="skip", dtype=str, sep=sep, engine='c')
            df['source'] = source

            if 'list_azimut' not in df.columns:
                df['list_azimut'] = pd.Series(pd.NA, index=df.index, dtype='string')
                for azimuth_column in ['list_azimut_last', 'list_azimut_old']:
                    if azimuth_column in df.columns:
                        candidate = df[azimuth_column].astype('string').replace(r'^\s*$', pd.NA, regex=True)
                        df['list_azimut'] = df['list_azimut'].fillna(candidate)
            
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
        
        def sort_key(tech: str) -> Tuple[float, int]:
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
        
        # Changements détectés (CHA, CHI, CHL, et combinaisons)
        mask_cha = df['source'] == 'comp_change.csv'
        # Utiliser la colonne action directement si elle existe
        if 'action' in df.columns and df[mask_cha]['action'].notna().any():
            result.loc[mask_cha] = df.loc[mask_cha, 'action']
        else:
            # Fallback si action n'est pas déjà définie
            result.loc[mask_cha] = 'CHA'
        
        # ==========================
        # AJO / ALL / AJA
        # ==========================

        mask_ajo = df['source'] == 'comp_added.csv'

        mask_activation = (
            mask_ajo &
            df['statut_y'].isin([
                "En service",
                "Techniquement opérationnel"
            ])
        )

        mask_activation_rt = (
            mask_activation &
            df['date_activ_y'].notna() &
            (df['date_activ_y'] < self.activation_limit_date)
        )

        result.loc[mask_activation_rt] = 'AJR'
        result.loc[
            mask_activation &
            ~mask_activation_rt
        ] = 'AJA'

        # Ajout seul
        result.loc[
            mask_ajo &
            ~mask_activation
        ] = 'AJO'

        
        # SUP pour comp_removed
        mask_sup = df['source'] == 'comp_removed.csv'
        result.loc[mask_sup] = 'SUP'
        
        # Logique complexe pour comp_modified
        mask_mod = df['source'] == 'comp_modified.csv'
        if mask_mod.any():
            mod_df = df.loc[mask_mod].copy()

            if 'action' in mod_df.columns:
                explicit_action = mod_df['action'].notna() & mod_df['action'].astype(str).str.strip().ne('')
                result.loc[mod_df.index[explicit_action]] = mod_df.loc[explicit_action, 'action']
            else:
                explicit_action = pd.Series(False, index=mod_df.index)
            
            # Vérifier la présence des colonnes nécessaires
            required_cols = ['statut_x', 'statut_y', 'date_activ_x', 'date_activ_y']
            missing_cols = [col for col in required_cols if col not in mod_df.columns]
            
            if missing_cols:
                functions_anfr.log_message(f"Colonnes manquantes pour determine_action: {missing_cols}", "WARN")
                result.loc[mask_mod] = "UNKNOWN"
                return result.fillna("UNKNOWN")
            
            statut_x = mod_df['statut_x']
            statut_y = mod_df['statut_y']
            date_activ_x = mod_df['date_activ_x']
            date_activ_y = mod_df['date_activ_y']
            
            # Conditions vectorisées avec gestion sécurisée des NaN
            cond_aav = (
                (statut_x == 'Projet approuvé') & 
                (statut_y == 'Projet approuvé') &
                (date_activ_x != date_activ_y)
            )

            cond_art = (
                (statut_x == 'Projet approuvé') &
                (statut_y.isin(['Techniquement opérationnel', 'En service'])) &
                date_activ_y.notna() &
                (date_activ_y < self.activation_limit_date)
            )
            
            cond_all = (
                (statut_x == 'Projet approuvé') &
                (statut_y.isin(['Techniquement opérationnel', 'En service'])) &
                ~cond_art
            )
            
            cond_ext = (
                (statut_x.isin(['En service', 'Techniquement opérationnel'])) &
                (statut_y == 'Projet approuvé')
            )
            
            # Appliquer les conditions avec les index corrects
            mod_indices = mod_df.index
            remaining = ~explicit_action
            result.loc[mod_indices[cond_aav & remaining]] = 'AAV'
            result.loc[mod_indices[cond_all & ~cond_aav & remaining]] = 'ALL'
            result.loc[mod_indices[cond_art & ~cond_aav & remaining]] = 'ART'
            result.loc[mod_indices[cond_ext & ~cond_aav & ~cond_all & remaining]] = 'EXT'
        
        return result.fillna("UNKNOWN")

    @staticmethod
    def format_azimuths(entries: pd.Series) -> str:
        """Retourne un azimut unique ou une liste annotée par fréquence."""
        pairs = []
        for entry in entries.dropna().astype(str):
            if '\t' not in entry:
                continue
            technology, azimuth = entry.split('\t', 1)
            technology = technology.strip()
            azimuth = azimuth.strip()
            if technology and azimuth and (technology, azimuth) not in pairs:
                pairs.append((technology, azimuth))

        if not pairs:
            return ''

        azimuths = {azimuth for _, azimuth in pairs}
        if len(azimuths) == 1:
            return next(iter(azimuths))

        return '; '.join(
            f'{technology}: {azimuth}'
            for technology, azimuth in sorted(pairs, key=lambda pair: pair[0])
        )

    @staticmethod
    def format_activation_dates(entries: pd.Series) -> str:
        """Retourne une date unique ou une date annotée par fréquence."""
        pairs = []
        for entry in entries.dropna().astype(str):
            if '\t' not in entry:
                continue
            technology, activation_date = entry.split('\t', 1)
            technology = technology.strip()
            activation_date = activation_date.strip()
            if technology and activation_date and (technology, activation_date) not in pairs:
                pairs.append((technology, activation_date))

        if not pairs:
            return ''

        dates = {activation_date for _, activation_date in pairs}
        if len(dates) == 1:
            return next(iter(dates))

        return '; '.join(
            f'{technology}: {activation_date}'
            for technology, activation_date in sorted(pairs, key=lambda pair: pair[0])
        )

    @staticmethod
    def format_azimuth_change(row: pd.Series) -> str:
        """Décrit la portée et les anciennes/nouvelles valeurs d'un CHZ."""
        if row.get('action') != 'CHZ':
            return ''
        scope = row.get('azimuth_scope') or 'fréquences'
        technology = row.get('technologie') or ''
        old_azimuth = row.get('list_azimut_old') or ''
        new_azimuth = row.get('list_azimut_last') or row.get('list_azimut') or ''
        label = scope if scope == 'site' else f'{scope}: {technology}'
        return f'CHZ ({label}) : {old_azimuth} -> {new_azimuth}'

    def extract_tech_dict_optimized(self, df: pd.DataFrame) -> Dict[Tuple[str, str], Set[str]]:
        """Version optimisée d'extract_tech_dict."""
        df_clean = df.dropna(subset=["sup_id", "adm_lb_nom", "emr_lb_systeme"])
        if df_clean.empty:
            return {}

        result = defaultdict(set)
        for sup_id, oper, tech in zip(
            df_clean["sup_id"],
            df_clean["adm_lb_nom"],
            df_clean["emr_lb_systeme"]
        ):
            result[(sup_id, oper)].add(tech)
        
        return dict(result)
    
    def format_technology_with_changes(self, techs_str: str, old_value: Optional[str], change_type: str) -> str:
        """Formate le champ technologie en intégrant les anciennes valeurs pour CHA/CHI/CHL.
        
        Args:
            techs_str: Technologies actuelles (liste séparée par ', ')
            old_value: Ancienne adresse (CHA), ancien ID support (CHI), ou anciennes coordonnées (CHL)
            change_type: Type de changement ('CHA', 'CHI', ou 'CHL')
        
        Returns:
            Chaîne formattée avec technologies et informations du changement
        """
        if not old_value or change_type not in ['CHA', 'CHI', 'CHL']:
            return techs_str
        
        old_value = str(old_value).strip()
        if not old_value:
            return techs_str
        
        # Créer le label en fonction du type de changement
        change_label = ""
        if change_type == 'CHA':
            change_label = f"[CHA: ancienne adresse = {old_value}]"
        elif change_type == 'CHI':
            change_label = f"[CHI: ancien support ID = {old_value}]"
        elif change_type == 'CHL':
            change_label = f"[CHL: anciennes coordonnées = {old_value}]"
        
        # Ajouter le label aux technologies
        if change_label:
            return f"{techs_str} {change_label}"
        return techs_str
    
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
        
        grouped = defaultdict(list)
        for key, statut in zip(zip(df_clean["sup_id"], df_clean["adm_lb_nom"]), df_clean["statut"]):
            grouped[key].append(statut)
        return grouped
    
    def is_zb_cached(self, support_id: str, operateur: str) -> bool:
        """Retourne le flag ZB calculé au niveau du site."""
        if operateur not in ZB_OPERATEURS:
            return False
        return self.zb_site_map.get(str(support_id).strip(), False)

    def build_zb_site_map(self, df_new: pd.DataFrame, df_old: pd.DataFrame) -> Dict[str, bool]:
        """Identifie les ZB à partir de l'ensemble du site, des technos et azimuts."""
        self.zb_site_map = {}
        old_supports = set(df_old['sup_id'].dropna().astype(str).str.strip()) if 'sup_id' in df_old.columns else set()
        required_columns = {'sup_id', 'adm_lb_nom', 'emr_lb_systeme', 'list_azimut'}
        if not required_columns.issubset(df_new.columns):
            return self.zb_site_map

        site_rows = df_new[list(required_columns)].dropna().drop_duplicates()
        for column in required_columns:
            site_rows[column] = site_rows[column].astype(str).str.strip()

        ops_by_site, techs_by_site, az_by_site = defaultdict(set), defaultdict(set), defaultdict(set)
        for sid, op, tech, az in zip(site_rows['sup_id'], site_rows['adm_lb_nom'],
                                    site_rows['emr_lb_systeme'], site_rows['list_azimut']):
            ops_by_site[sid].add(op); techs_by_site[sid].add(tech); az_by_site[sid].add(az)

        for support_id, operators in ops_by_site.items():
            technologies = techs_by_site[support_id]
            azimuths = az_by_site[support_id]
            techs_ok = ZB_CORE_TECHNOS <= technologies <= ZB_TECHNOS   # noyau obligatoire, 1800 optionnel
            valid_site = techs_ok and len(azimuths) == 1 and operators == ZB_OPERATEURS
            first_declaration = (
                support_id not in old_supports and len(operators) == 1 and operators <= ZB_OPERATEURS
            )
            self.zb_site_map[support_id] = valid_site or (
                first_declaration and techs_ok and len(azimuths) == 1
            )
        return self.zb_site_map
    
    def is_new_cached(self, support_id: str, operateur: str) -> bool:
        """Version mise en cache de is_new."""
        key = (str(support_id).strip(), operateur)
        
        if key in self._new_cache:
            return self._new_cache[key]
        
        statuts = self.new_status_dict.get(key, [])
        result = not statuts or all(stat == "Projet approuvé" for stat in statuts)
        
        self._new_cache[key] = result
        return result
    
    def find_and_isolate_duplicates_optimized(self, df, location_threshold=0.001,
                                            address_similarity_threshold=0.5):
        if len(df) < 2:
            return pd.DataFrame()
        xy = df['coordonnees'].str.split(', ', expand=True).astype(float).to_numpy()
        grid_size = location_threshold * 5
        gx = (xy[:, 0] / grid_size).astype(int)
        gy = (xy[:, 1] / grid_size).astype(int)
        grid: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
        for pos, key in enumerate(zip(gx.tolist(), gy.tolist())):
            grid[key].append(pos)
        addr = [frozenset(a.lower().split()) for a in df['adresse']]
        tech = [frozenset(t.split(", ")) for t in df['technologie']]
        oper = df['operateur'].tolist()
        act = df['action'].tolist()
        processed = set()
        for pos in range(len(df)):
            if pos in processed:
                continue
            x, y = int(gx[pos]), int(gy[pos])
            cand_pos = [c for dx in (-1, 0, 1) for dy in (-1, 0, 1)
                for c in grid.get((x + dx, y + dy), ()) if c != pos and c not in processed]
            if not cand_pos:
                continue
            cand = np.array(cand_pos, dtype=np.intp)
            dist = np.sqrt(((xy[cand] - xy[pos]) ** 2).sum(axis=1))
            for c in cand[dist <= location_threshold].tolist():
                union = addr[pos] | addr[c]
                sim = len(addr[pos] & addr[c]) / len(union) if union else 0
                if sim < address_similarity_threshold:
                    continue
                if tech[pos] == tech[c] and oper[pos] == oper[c] and act[pos] != act[c]:
                    processed.update((pos, c))
        return df.iloc[sorted(processed)] if processed else pd.DataFrame()
    
    def merge_and_process_optimized(self, added_path: str, modified_path: str, 
                                  removed_path: str, output_path: str) -> None:
        """Version optimisée de merge_and_process."""
        try:
            # Chargement optimisé des fichiers
            added_df = self.preprocess_csv_optimized(added_path, 'comp_added.csv', sep=',')
            modified_df = self.preprocess_csv_optimized(modified_path, 'comp_modified.csv', sep=',')
            removed_df = self.preprocess_csv_optimized(removed_path, 'comp_removed.csv', sep=',')

            if added_df.empty and modified_df.empty and removed_df.empty:
                functions_anfr.log_message("Tous les fichiers sont vides.", "FATAL")
                raise SystemExit(1)
            
            # === DÉTECTION DES CHANGEMENTS: CHA, CHI, CHL, et combinaisons ===
            change_dfs = {}  # Dict pour stocker les différents types de changements
            indices_to_remove_added = []
            indices_to_remove_removed = []
            
            def parse_coords(coord_str):
                """Parse 'lat , lon' format et retourne (lat, lon) en float, ou (None, None)"""
                if pd.isna(coord_str):
                    return (None, None)
                try:
                    # Normaliser le format et split
                    coord_str = str(coord_str).replace(' ', '')
                    parts = coord_str.split(',')
                    if len(parts) == 2:
                        return (float(parts[0]), float(parts[1]))
                except:
                    pass
                return (None, None)
            
            def coord_distance_meters(lat1, lon1, lat2, lon2):
                """Approximation simple de la distance entre deux points en mètres
                Utilise la formule de Haversine simplifiée"""
                if lat1 is None or lat2 is None:
                    return None
                R = 6371000  # Rayon terrestre en mètres
                dlat = math.radians(lat2 - lat1)
                dlon = math.radians(lon2 - lon1)
                a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
                c = 2 * math.asin(math.sqrt(a))
                return R * c
            
            if not added_df.empty and not removed_df.empty:
                # === Détection de CHA: même ID support, opérateur, techno, coords → adresse change ===
                # Merge sur id_support + operateur + technologie + coordonnees
                merge_cols_cha = ['id_support', 'operateur', 'technologie', 'code_insee', 'coordonnees']
                available_cols_cha = [col for col in merge_cols_cha if col in added_df.columns and col in removed_df.columns]
                
                if available_cols_cha == merge_cols_cha:
                    removed_cha = removed_df[available_cols_cha + ['adresse0', 'adresse1', 'adresse2', 'adresse3']].copy()
                    added_cha = added_df[available_cols_cha + ['adresse0', 'adresse1', 'adresse2', 'adresse3']].copy()
                    removed_cha['_idx_rem'] = removed_df.index
                    added_cha['_idx_add'] = added_df.index
                    
                    matched_cha = pd.merge(removed_cha, added_cha, on=available_cols_cha, how='inner', suffixes=('_rem', '_add'))
                    
                    if not matched_cha.empty:
                        # Vérifier que les adresses sont différentes
                        addr_diff_mask = pd.Series(False, index=matched_cha.index)
                        for col in ['adresse0', 'adresse1', 'adresse2', 'adresse3']:
                            col_rem = f'{col}_rem'
                            col_add = f'{col}_add'
                            rem_vals = matched_cha[col_rem].fillna('').astype(str).str.strip()
                            add_vals = matched_cha[col_add].fillna('').astype(str).str.strip()
                            addr_diff_mask = addr_diff_mask | (rem_vals != add_vals)
                        
                        matched_cha_filtered = matched_cha[addr_diff_mask].copy()
                        if not matched_cha_filtered.empty:
                            idx_rem = matched_cha_filtered['_idx_rem'].tolist()
                            idx_add = matched_cha_filtered['_idx_add'].tolist()
                            indices_to_remove_removed.extend(idx_rem)
                            indices_to_remove_added.extend(idx_add)
                            
                            change_df = added_df.loc[idx_add].copy()
                            change_df['source'] = 'comp_change.csv'
                            change_df['action'] = 'CHA'
                            
                            # Ajouter les anciennes adresses dans une colonne dédiée
                            old_addrs = []
                            for i, (idx_a, idx_r) in enumerate(zip(idx_add, idx_rem)):
                                old_row = removed_df.loc[idx_r]
                                # Construire l'ancienne adresse en excluant les valeurs NaN/vides
                                old_addr_parts = []
                                for col in ['adresse0', 'adresse1', 'adresse2', 'adresse3']:
                                    val = old_row.get(col, '')
                                    # Convertir en string et nettoyer
                                    val_str = str(val).strip() if pd.notna(val) else ''
                                    # Ignorer les 'nan' et les valeurs vides
                                    if val_str and val_str != 'nan':
                                        old_addr_parts.append(val_str)
                                old_addr = ' '.join(old_addr_parts) if old_addr_parts else ''
                                old_addrs.append(old_addr)
                            change_df['old_address'] = old_addrs
                            
                            change_dfs['CHA'] = change_df
                            functions_anfr.log_message(f"Détecté {len(change_df)} changements CHA.")
                
                # === Détection de CHI GÉOGRAPHIQUE: sites proches avec ID différent (fusion multiple changements) ===
                # Cette détection capture les cas où un support change d'ID mais reste géographiquement au même endroit
                # avec possibilité de changements d'adresse, hauteur, propriétaire, etc.
                if 'coordonnees' in added_df.columns and 'coordonnees' in removed_df.columns:
                    merge_cols_chi_geo = ['operateur', 'technologie', 'code_insee']
                    available_cols_chi_geo = [col for col in merge_cols_chi_geo if col in added_df.columns and col in removed_df.columns]
                    
                    if available_cols_chi_geo == merge_cols_chi_geo:
                        # Merge large sur opérateur + techno + code_insee
                        removed_chi_geo = removed_df[available_cols_chi_geo + ['id_support', 'coordonnees']].copy()
                        added_chi_geo = added_df[available_cols_chi_geo + ['id_support', 'coordonnees']].copy()
                        removed_chi_geo['_idx_rem'] = removed_df.index
                        added_chi_geo['_idx_add'] = added_df.index
                        
                        matched_chi_geo = pd.merge(removed_chi_geo, added_chi_geo, on=available_cols_chi_geo, how='inner', suffixes=('_rem', '_add'))
                        
                        if not matched_chi_geo.empty:
                            # Filtrer sur proximité géographique (< 100m) et ID différent
                            chi_geo_matches = []
                            for idx, row in matched_chi_geo.iterrows():
                                if row['id_support_rem'] == row['id_support_add']:
                                    continue  # Même ID, pas intéressant
                                
                                # Calculer distance entre les deux points
                                try:
                                    lat1, lon1 = parse_coords(row['coordonnees_rem'])
                                    lat2, lon2 = parse_coords(row['coordonnees_add'])
                                    
                                    if lat1 is not None and lat2 is not None:
                                        dist = coord_distance_meters(lat1, lon1, lat2, lon2)
                                        if dist is not None and dist < 100:  # Moins de 100m
                                            chi_geo_matches.append(idx)
                                except:
                                    pass
                            
                            if chi_geo_matches:
                                matched_chi_geo_filtered = matched_chi_geo.loc[chi_geo_matches].copy()
                                
                                if not matched_chi_geo_filtered.empty:
                                    # CORRECTION : Créer un mapping idx_add -> idx_rem AVANT le filtrage
                                    mapping_add_to_rem = dict(zip(
                                        matched_chi_geo_filtered['_idx_add'],
                                        matched_chi_geo_filtered['_idx_rem']
                                    ))
                                    
                                    idx_rem = matched_chi_geo_filtered['_idx_rem'].tolist()
                                    idx_add = matched_chi_geo_filtered['_idx_add'].tolist()
                                    
                                    # Éviter les doublons avec CHA
                                    idx_rem_filtered = [i for i in idx_rem if i not in indices_to_remove_removed]
                                    idx_add_filtered = [i for i in idx_add if i not in indices_to_remove_added]
                                    
                                    # IMPORTANT : Garder seulement les paires cohérentes après filtrage
                                    valid_pairs = []
                                    for idx_a in idx_add_filtered:
                                        idx_r = mapping_add_to_rem.get(idx_a)
                                        if idx_r is not None and idx_r in idx_rem_filtered:
                                            valid_pairs.append((idx_a, idx_r))
                                    
                                    if valid_pairs:
                                        idx_add_final = [pair[0] for pair in valid_pairs]
                                        idx_rem_final = [pair[1] for pair in valid_pairs]
                                        
                                        indices_to_remove_removed.extend(idx_rem_final)
                                        indices_to_remove_added.extend(idx_add_final)
                                        
                                        change_df = added_df.loc[idx_add_final].copy()
                                        change_df['source'] = 'comp_change.csv'
                                        change_df['action'] = 'CHI'
                                        
                                        # Ajouter les anciens IDs de support (maintenant alignés)
                                        old_ids = [removed_df.loc[idx_r, 'id_support'] for idx_r in idx_rem_final]
                                        
                                        change_df = change_df.reset_index(drop=True)
                                        change_df['old_id_support'] = old_ids
                                        
                                        change_dfs['CHI'] = change_df
                                        functions_anfr.log_message(f"Détecté {len(change_df)} changements CHI (géographique).")
                
                # === Détection de CHI: même opérateur, techno, coords, adresses → ID change ===
                merge_cols_chi = ['operateur', 'technologie', 'code_insee', 'coordonnees']
                available_cols_chi = [col for col in merge_cols_chi if col in added_df.columns and col in removed_df.columns]
                
                if available_cols_chi == merge_cols_chi:
                    removed_chi = removed_df[available_cols_chi + ['id_support', 'adresse0', 'adresse1', 'adresse2', 'adresse3']].copy()
                    added_chi = added_df[available_cols_chi + ['id_support', 'adresse0', 'adresse1', 'adresse2', 'adresse3']].copy()
                    removed_chi['_idx_rem'] = removed_df.index
                    added_chi['_idx_add'] = added_df.index
                    
                    matched_chi = pd.merge(removed_chi, added_chi, on=available_cols_chi, how='inner', suffixes=('_rem', '_add'))
                    
                    if not matched_chi.empty:
                        # Vérifier ID différent, adresses identiques
                        id_diff = matched_chi['id_support_rem'].astype(str) != matched_chi['id_support_add'].astype(str)
                        addr_same = pd.Series(True, index=matched_chi.index)
                        
                        for col in ['adresse0', 'adresse1', 'adresse2', 'adresse3']:
                            col_rem = f'{col}_rem'
                            col_add = f'{col}_add'
                            rem_vals = matched_chi[col_rem].fillna('').astype(str).str.strip()
                            add_vals = matched_chi[col_add].fillna('').astype(str).str.strip()
                            addr_same = addr_same & (rem_vals == add_vals)
                        
                        matched_chi_filtered = matched_chi[id_diff & addr_same].copy()
                        if not matched_chi_filtered.empty:
                            idx_rem = matched_chi_filtered['_idx_rem'].tolist()
                            idx_add = matched_chi_filtered['_idx_add'].tolist()
                            # Éviter les doublons avec CHA
                            idx_rem = [i for i in idx_rem if i not in indices_to_remove_removed]
                            idx_add = [i for i in idx_add if i not in indices_to_remove_added]
                            
                            if idx_add and idx_rem:
                                indices_to_remove_removed.extend(idx_rem)
                                indices_to_remove_added.extend(idx_add)
                                
                                change_df = added_df.loc[idx_add].copy()
                                change_df['source'] = 'comp_change.csv'
                                change_df['action'] = 'CHI'
                                
                                # Ajouter les anciens IDs de support dans une colonne dédiée
                                old_ids = []
                                for idx_r in idx_rem:
                                    old_id = removed_df.loc[idx_r, 'id_support']
                                    old_ids.append(old_id)
                                change_df['old_id_support'] = old_ids
                                
                                change_dfs['CHI'] = change_df
                                functions_anfr.log_message(f"Détecté {len(change_df)} changements CHI.")
                
                # === Détection de CHL: même ID support, opérateur, techno, adresses → coordonnees changent ===
                merge_cols_chl = ['id_support', 'operateur', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3']
                available_cols_chl = [col for col in merge_cols_chl if col in added_df.columns and col in removed_df.columns]
                
                if available_cols_chl == merge_cols_chl:
                    removed_chl = removed_df[available_cols_chl + ['code_insee', 'coordonnees']].copy()
                    added_chl = added_df[available_cols_chl + ['code_insee', 'coordonnees']].copy()
                    removed_chl['_idx_rem'] = removed_df.index
                    added_chl['_idx_add'] = added_df.index
                    
                    matched_chl = pd.merge(removed_chl, added_chl, on=available_cols_chl, how='inner', suffixes=('_rem', '_add'))
                    
                    if not matched_chl.empty:
                        # Vérifier coordonnées différentes
                        coord_diff_mask = pd.Series(False, index=matched_chl.index)
                        
                        coord_diff_idx = []

                        for idx, row in matched_chl.iterrows():
                            lat1, lon1 = parse_coords(row['coordonnees_rem'])
                            lat2, lon2 = parse_coords(row['coordonnees_add'])

                            if lat1 is not None and lat2 is not None:
                                try:
                                    dist = coord_distance_meters(lat1, lon1, lat2, lon2)
                                    if dist is not None and dist >= 50:  # Seuil de 50 mètres
                                        coord_diff_idx.append(idx)
                                except Exception:
                                    pass
                            elif lat1 != lat2 or lon1 != lon2:
                                coord_diff_idx.append(idx)

                        coord_diff_mask = matched_chl.index.isin(coord_diff_idx)
                        matched_chl_filtered = matched_chl[coord_diff_mask].copy()

                        if not matched_chl_filtered.empty:
                            idx_rem = matched_chl_filtered['_idx_rem'].tolist()
                            idx_add = matched_chl_filtered['_idx_add'].tolist()
                            # Éviter les doublons
                            idx_rem = [i for i in idx_rem if i not in indices_to_remove_removed]
                            idx_add = [i for i in idx_add if i not in indices_to_remove_added]
                            
                            if idx_add and idx_rem:
                                indices_to_remove_removed.extend(idx_rem)
                                indices_to_remove_added.extend(idx_add)
                                
                                change_df = added_df.loc[idx_add].copy()
                                change_df['source'] = 'comp_change.csv'
                                change_df['action'] = 'CHL'
                                
                                # Ajouter les anciennes coordonnées dans une colonne dédiée
                                old_coords = matched_chl_filtered['coordonnees_rem'].tolist()
                                change_df['old_coordonnees'] = old_coords
                                
                                change_dfs['CHL'] = change_df
                                functions_anfr.log_message(f"Détecté {len(change_df)} changements CHL.")

            # === Détection de CHT: changement de type de support ===
            if not added_df.empty and not removed_df.empty:
                # Colonnes communes pour le merge (SANS type_support)
                merge_cols_cht = ['id_support', 'operateur', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee', 'coordonnees']
                available_cols_cht = [col for col in merge_cols_cht if col in added_df.columns and col in removed_df.columns]
                
                # Vérifier que type_support existe dans les deux DataFrames
                if (available_cols_cht == merge_cols_cht and 
                    'type_support' in removed_df.columns and 
                    'type_support' in added_df.columns):
                    
                    removed_cht = removed_df[available_cols_cht + ['type_support']].copy()
                    added_cht = added_df[available_cols_cht + ['type_support']].copy()
                    removed_cht['_idx_rem'] = removed_df.index
                    added_cht['_idx_add'] = added_df.index
                    
                    matched_cht = pd.merge(removed_cht, added_cht, on=available_cols_cht, how='inner', suffixes=('_rem', '_add'))
                    
                    if not matched_cht.empty:
                        # Normaliser les valeurs avant comparaison
                        type_rem = matched_cht['type_support_rem'].fillna('').astype(str).str.strip()
                        type_add = matched_cht['type_support_add'].fillna('').astype(str).str.strip()
                        type_diff_mask = type_rem != type_add
                        
                        matched_cht_filtered = matched_cht[type_diff_mask].copy()
                        
                        if not matched_cht_filtered.empty:
                            idx_rem = matched_cht_filtered['_idx_rem'].tolist()
                            idx_add = matched_cht_filtered['_idx_add'].tolist()
                            # Éviter les doublons
                            idx_rem = [i for i in idx_rem if i not in indices_to_remove_removed]
                            idx_add = [i for i in idx_add if i not in indices_to_remove_added]
                            
                            if idx_add and idx_rem:
                                indices_to_remove_removed.extend(idx_rem)
                                indices_to_remove_added.extend(idx_add)
                                
                                change_df = added_df.loc[idx_add].copy()
                                change_df['source'] = 'comp_change.csv'
                                change_df['action'] = 'CHT'
                                
                                # Convertir et ajouter les anciens types de support
                                old_types = []
                                for idx_r in idx_rem:
                                    try:
                                        type_val = removed_df.loc[idx_r, 'type_support']
                                        # Gérer les valeurs vides/NaN
                                        if pd.isna(type_val) or str(type_val).strip() == '':
                                            old_types.append("Inconnu")
                                            continue
                                        
                                        type_str = str(type_val).strip()
                                        # Convertir en int si c'est un nombre
                                        if type_str.replace('.','').replace('-','').isdigit():
                                            type_int = int(float(type_str))
                                            type_converted = CORRESPONDANCES_TYPE_SUPPORT.get(type_int, "Inconnu")
                                        else:
                                            type_converted = "Inconnu"
                                    except:
                                        type_converted = "Inconnu"
                                    old_types.append(type_converted)
                                
                                change_df['old_type_support'] = old_types
                                change_dfs['CHT'] = change_df
                                functions_anfr.log_message(f"Détecté {len(change_df)} changements CHT.")
            
            # === Détection de CHP: changement de propriétaire de support ===
            if not added_df.empty and not removed_df.empty:
                merge_cols_chp = ['id_support', 'operateur', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee', 'coordonnees']
                available_cols_chp = [col for col in merge_cols_chp if col in added_df.columns and col in removed_df.columns]
                
                if (available_cols_chp == merge_cols_chp and 
                    'proprietaire_support' in removed_df.columns and 
                    'proprietaire_support' in added_df.columns):
                    
                    removed_chp = removed_df[available_cols_chp + ['proprietaire_support']].copy()
                    added_chp = added_df[available_cols_chp + ['proprietaire_support']].copy()
                    removed_chp['_idx_rem'] = removed_df.index
                    added_chp['_idx_add'] = added_df.index
                    
                    matched_chp = pd.merge(removed_chp, added_chp, on=available_cols_chp, how='inner', suffixes=('_rem', '_add'))
                    
                    if not matched_chp.empty:
                        # Normaliser les valeurs avant comparaison
                        prop_rem = matched_chp['proprietaire_support_rem'].fillna('').astype(str).str.strip()
                        prop_add = matched_chp['proprietaire_support_add'].fillna('').astype(str).str.strip()
                        prop_diff_mask = prop_rem != prop_add
                        
                        matched_chp_filtered = matched_chp[prop_diff_mask].copy()
                        
                        if not matched_chp_filtered.empty:
                            idx_rem = matched_chp_filtered['_idx_rem'].tolist()
                            idx_add = matched_chp_filtered['_idx_add'].tolist()
                            # Éviter les doublons
                            idx_rem = [i for i in idx_rem if i not in indices_to_remove_removed]
                            idx_add = [i for i in idx_add if i not in indices_to_remove_added]
                            
                            if idx_add and idx_rem:
                                indices_to_remove_removed.extend(idx_rem)
                                indices_to_remove_added.extend(idx_add)
                                
                                change_df = added_df.loc[idx_add].copy()
                                change_df['source'] = 'comp_change.csv'
                                change_df['action'] = 'CHP'
                                
                                # Convertir et ajouter les anciens propriétaires
                                old_props = []
                                for idx_r in idx_rem:
                                    try:
                                        prop_val = removed_df.loc[idx_r, 'proprietaire_support']
                                        if pd.isna(prop_val) or str(prop_val).strip() == '':
                                            old_props.append("Inconnu")
                                            continue
                                        
                                        prop_str = str(prop_val).strip()
                                        if prop_str.replace('.','').replace('-','').isdigit():
                                            prop_int = int(float(prop_str))
                                            prop_converted = CORRESPONDANCES_PROPRIETAIRE_SUPPORT.get(prop_int, "Inconnu")
                                        else:
                                            prop_converted = "Inconnu"
                                    except:
                                        prop_converted = "Inconnu"
                                    old_props.append(prop_converted)
                                
                                change_df['old_proprietaire_support'] = old_props
                                change_dfs['CHP'] = change_df
                                functions_anfr.log_message(f"Détecté {len(change_df)} changements CHP.")
            
            # === Détection de CHH: changement de hauteur de support ===
            if not added_df.empty and not removed_df.empty:
                merge_cols_chh = ['id_support', 'operateur', 'technologie', 'adresse0', 'adresse1', 'adresse2', 'adresse3', 'code_insee', 'coordonnees']
                available_cols_chh = [col for col in merge_cols_chh if col in added_df.columns and col in removed_df.columns]
                
                if (available_cols_chh == merge_cols_chh and 
                    'hauteur_support' in removed_df.columns and 
                    'hauteur_support' in added_df.columns):
                    
                    removed_chh = removed_df[available_cols_chh + ['hauteur_support']].copy()
                    added_chh = added_df[available_cols_chh + ['hauteur_support']].copy()
                    removed_chh['_idx_rem'] = removed_df.index
                    added_chh['_idx_add'] = added_df.index
                    
                    matched_chh = pd.merge(removed_chh, added_chh, on=available_cols_chh, how='inner', suffixes=('_rem', '_add'))
                    
                    if not matched_chh.empty:
                        # Normaliser et comparer les hauteurs
                        hauteur_rem = matched_chh['hauteur_support_rem'].fillna('0').astype(str).str.strip()
                        hauteur_add = matched_chh['hauteur_support_add'].fillna('0').astype(str).str.strip()
                        hauteur_diff_mask = hauteur_rem != hauteur_add
                        
                        matched_chh_filtered = matched_chh[hauteur_diff_mask].copy()
                        
                        if not matched_chh_filtered.empty:
                            idx_rem = matched_chh_filtered['_idx_rem'].tolist()
                            idx_add = matched_chh_filtered['_idx_add'].tolist()
                            # Éviter les doublons
                            idx_rem = [i for i in idx_rem if i not in indices_to_remove_removed]
                            idx_add = [i for i in idx_add if i not in indices_to_remove_added]
                            
                            if idx_add and idx_rem:
                                indices_to_remove_removed.extend(idx_rem)
                                indices_to_remove_added.extend(idx_add)
                                
                                change_df = added_df.loc[idx_add].copy()
                                change_df['source'] = 'comp_change.csv'
                                change_df['action'] = 'CHH'
                                
                                # Ajouter les anciennes hauteurs (format avec virgule et 'm')
                                old_hauteurs = []
                                for idx_r in idx_rem:
                                    h_val = removed_df.loc[idx_r, 'hauteur_support']
                                    h_str = str(h_val) if pd.notna(h_val) else '0'
                                    h_formatted = f"{h_str.replace('.', ',')}m"
                                    old_hauteurs.append(h_formatted)
                                
                                change_df['old_hauteur_support'] = old_hauteurs
                                change_dfs['CHH'] = change_df
                                functions_anfr.log_message(f"Détecté {len(change_df)} changements CHH.")

            # Retirer les doublons d'indices
            indices_to_remove_added = list(set(indices_to_remove_added))
            indices_to_remove_removed = list(set(indices_to_remove_removed))
            
            if indices_to_remove_removed:
                removed_df = removed_df.drop(indices_to_remove_removed)

            if indices_to_remove_added:
                added_df = added_df.drop(indices_to_remove_added)

            # Une fréquence remplacée par une autre reste une modification du
            # site lorsque le nombre de secteurs ne change pas.
            replacement_dfs = []
            if not added_df.empty and not removed_df.empty:
                def sector_count(value):
                    if pd.isna(value) or not str(value).strip():
                        return 0
                    return len({part.strip() for part in str(value).split('|') if part.strip()})

                def azimuth_set(value):
                        if pd.isna(value):
                            return frozenset()
                        return frozenset(p.strip() for p in str(value).split('|') if p.strip())

                site_columns = ['id_support', 'operateur']
                for site_key, added_site in added_df.groupby(site_columns):
                    removed_site = removed_df[
                        (removed_df['id_support'] == site_key[0]) &
                        (removed_df['operateur'] == site_key[1])
                    ]
                    if len(added_site) != 1 or len(removed_site) != 1:
                        continue

                    added_index, added_row = next(added_site.iterrows())
                    removed_index, removed_row = next(removed_site.iterrows())
                    added_azimuth = added_row.get('list_azimut_last') or added_row.get('list_azimut')
                    removed_azimuth = removed_row.get('list_azimut_old') or removed_row.get('list_azimut')
                    if sector_count(added_azimuth) == 0 or sector_count(added_azimuth) != sector_count(removed_azimuth):
                        continue

                    if azimuth_set(added_azimuth) == azimuth_set(removed_azimuth):
                        continue  # mêmes azimuts : simple changement de fréquence, pas un CHZ

                    replacement = added_row.to_frame().T.copy()
                    replacement['source'] = 'comp_change.csv'
                    replacement['action'] = 'CHZ'
                    replacement['technologie'] = added_row['technologie']
                    replacement['list_azimut_old'] = removed_azimuth
                    replacement['list_azimut_last'] = added_azimuth
                    replacement['list_azimut'] = added_azimuth
                    replacement['azimuth_scope'] = 'fréquences'
                    replacement['azimuth_replacement'] = True
                    replacement['replacement_old_technology'] = removed_row['technologie']
                    replacement['replacement_new_technology'] = added_row['technologie']
                    replacement_dfs.append(replacement)
                    indices_to_remove_added.append(added_index)
                    indices_to_remove_removed.append(removed_index)

                if replacement_dfs:
                    added_df = added_df.drop(indices_to_remove_added, errors='ignore')
                    removed_df = removed_df.drop(indices_to_remove_removed, errors='ignore')
            
            # Détermination des actions de manière vectorisée
            all_dfs = [added_df, modified_df, removed_df]
            all_dfs.extend(replacement_dfs)
            for df in all_dfs:
                if not df.empty:
                    df['action'] = self.determine_action_vectorized(df)
                    # Initialiser la colonne infos (vide par défaut)
                    df['infos'] = None
                    if 'list_azimut' not in df.columns:
                        df['list_azimut'] = None
            
            # Ajouter les changements détectés à la liste
            for change_type, change_df in change_dfs.items():
                if not change_df.empty:
                    # Remplir la colonne infos selon le type de changement
                    if change_type == 'CHA':
                        change_df['infos'] = change_df['old_address']
                    elif change_type == 'CHI':
                        change_df['infos'] = change_df['old_id_support']
                    elif change_type == 'CHL':
                        change_df['infos'] = change_df['old_coordonnees']
                    elif change_type == 'CHT':
                        change_df['infos'] = change_df['old_type_support']
                    elif change_type == 'CHP':
                        change_df['infos'] = change_df['old_proprietaire_support']
                    elif change_type == 'CHH':
                        change_df['infos'] = change_df['old_hauteur_support']
                    
                    # Vider la colonne technologie pour tous les changements
                    change_df['technologie'] = ''
                    
                    # NE PAS SUPPRIMER les colonnes old_* ICI
                    # Elles seront supprimées après la concaténation
                    
                    all_dfs.append(change_df)
            
            # Concaténation
            final_df = pd.concat([df for df in all_dfs if not df.empty], ignore_index=True)

            t, a = final_df['technologie'], final_df['list_azimut']
            final_df['azimuth_entry'] = (t.astype(str) + '\t' + a.astype(str)).where(t.notna() & a.notna(), '')
            final_df.loc[final_df['action'] == 'CHZ', 'infos'] = final_df.loc[
                final_df['action'] == 'CHZ'
            ].apply(self.format_azimuth_change, axis=1)
            
            # S'assurer que la colonne infos existe après concaténation
            if 'infos' not in final_df.columns:
                final_df['infos'] = None
            
            # AJOUT : Supprimer les colonnes temporaires old_* APRÈS concaténation
            old_cols_to_drop = [
                'old_address', 'old_id_support', 'old_coordonnees',
                'old_type_support', 'old_proprietaire_support', 'old_hauteur_support'
            ]
            final_df = final_df.drop(columns=old_cols_to_drop, errors='ignore')
            
            # Uniformisation des colonnes avec combine_first vectorisé
            # Gérer les colonnes avec suffixes _x et _y seulement si elles existent
            # Pour type_support, hauteur_support, proprietaire_support : elles existent déjà sans suffixe
            combine_cols = {
                'date_activ': ['date_activ_y', 'date_activ_x'],
                'statut': ['statut_x', 'statut_y']
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
            
            # Vérifier que les colonnes type_support, hauteur_support et proprietaire_support existent
            for col in ['type_support', 'hauteur_support', 'proprietaire_support']:
                if col not in final_df.columns:
                    final_df[col] = None

            # Entrées date par techno, calculées APRÈS la consolidation de date_activ
            date_mask = final_df['technologie'].notna() & final_df['date_activ'].notna()
            final_df['activation_date_entry'] = ''
            final_df.loc[date_mask, 'activation_date_entry'] = (
                final_df.loc[date_mask, 'technologie'].astype(str) + '\t'
                + final_df.loc[date_mask, 'date_activ'].astype(str)
            )

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
                'action': 'first',
                'infos': lambda values: '; '.join(dict.fromkeys(
                    value for value in values.dropna().astype(str) if value
                )),
                'azimuth_entry': self.format_azimuths,
                'activation_date_entry': self.format_activation_dates
            }
            
            final_df = (final_df.groupby(['id_support', 'operateur', 'action'], as_index=False)
                       .agg(agg_dict))

            final_df = final_df.rename(columns={
                'azimuth_entry': 'liste_azimut',
                'activation_date_entry': 'date_activ'
            })

            chz_mask = final_df['action'] == 'CHZ'
            if chz_mask.any() and self.new_azimuth_map:
                final_df.loc[chz_mask, 'liste_azimut'] = final_df.loc[chz_mask].apply(
                    lambda row: self.format_azimuths(
                        pd.Series(self.new_azimuth_map.get((row['id_support'], row['operateur']), []))
                    ),
                    axis=1
                )
            
            # Post-traitement du champ technologie
            # Pour CHA/CHI/CHL : vider la technologie (déjà fait avant, mais on s'assure)
            # Pour les autres actions : trier les technologies normalement
            mask_change = final_df['action'].isin(['CHA', 'CHI', 'CHL', 'CHT', 'CHP', 'CHH'])
            
            # Vider la technologie pour les changements
            final_df.loc[mask_change, 'technologie'] = ''
            
            # Trier les technologies pour les autres actions
            final_df.loc[~mask_change, 'technologie'] = final_df.loc[~mask_change, 'technologie'].apply(
                self.sort_technologies_optimized
            )
            
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
            r = coords_split.round(4).astype(str)
            final_df['coordonnees'] = r.iloc[:, 0].str.cat(r.iloc[:, 1:], sep=',')
            
            # Calculs par batch pour is_zb
            keys = list(zip(final_df['id_support'], final_df['operateur']))
            zb_results = {k: self.is_zb_cached(*k) for k in set(keys)}
            new_results = {k: self.is_new_cached(*k) for k in set(keys)}
            final_df['is_zb'] = [zb_results[k] for k in keys]
            final_df['is_new'] = [new_results[k] for k in keys]
            
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
            data_date = DATA_DATE or None
            if not data_date:
                metadata = functions_anfr.parse_anfr_filename(NEW_CSV_PATH)
                data_date = metadata[2] if metadata else None
            time_period = functions_anfr.get_period_code(TIMESTAMP, args.update_type, data_date)
            final_df.to_csv(os.path.join(output_path, f"{time_period}.csv"), index=False)
            
            functions_anfr.log_message("Fichiers finaux générés avec succès, duplications supprimées.")
            
        except Exception as e:
            functions_anfr.log_message(f"Échec lors du traitement des fichiers - {e}", "FATAL")
            raise SystemExit(1)


def main(no_insee, no_process, debug):
    """Fonction principale optimisée."""
    processor = OptimizedProcessor(update_type=args.update_type)
    
    path_app = os.path.dirname(os.path.abspath(__file__))
    added_path = os.path.join(path_app, 'files', 'compared', 'comp_added.csv')
    modified_path = os.path.join(path_app, 'files', 'compared', 'comp_modified.csv')
    removed_path = os.path.join(path_app, 'files', 'compared', 'comp_removed.csv')
    insee_path = os.path.join(path_app, 'files', 'cc_insee', 'cc_insee.csv')
    pretraite_path = os.path.join(path_app, 'files', 'pretraite')

    # Chargement des données une seule fois au début
    functions_anfr.log_message("Début du chargement des fichiers CSV principaux...", "INFO")
    NEEDED_COLS = {"sup_id", "adm_lb_nom", "emr_lb_systeme", "list_azimut", "statut"}
    try:
        # Chargement complet pour extract_tech_dict et build_new_status_map
        functions_anfr.log_message(f"Chargement de {os.path.basename(OLD_CSV_PATH)}...", "INFO")
        sep_o = functions_anfr.detect_separator(OLD_CSV_PATH)
        df_old = pd.read_csv(OLD_CSV_PATH, on_bad_lines="skip", dtype=str, sep=sep_o, engine='c',
                     usecols=lambda c: c in NEEDED_COLS)
        functions_anfr.log_message(f"✓ {os.path.basename(OLD_CSV_PATH)} chargé ({len(df_old):,} lignes)", "INFO")
        
        functions_anfr.log_message(f"Chargement de {os.path.basename(NEW_CSV_PATH)}...", "INFO")
        sep_n = functions_anfr.detect_separator(NEW_CSV_PATH)
        df_new = pd.read_csv(NEW_CSV_PATH, on_bad_lines="skip", dtype=str, sep=sep_n, engine='c',
                     usecols=lambda c: c in NEEDED_COLS)
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
    processor.build_zb_site_map(df_new, df_old)
    functions_anfr.log_message(
        f"✓ Index ZB sites créé ({sum(processor.zb_site_map.values()):,} sites)",
        "INFO"
    )
    
    if new_has_tech_cols:
        processor.techs_new_map = processor.extract_tech_dict_optimized(df_new)
        functions_anfr.log_message(f"✓ Index tech NEW créé ({len(processor.techs_new_map):,} entrées)", "INFO")
        if 'list_azimut' in df_new.columns:
            d = df_new.dropna(subset=['sup_id', 'adm_lb_nom', 'emr_lb_systeme', 'list_azimut'])
            for key, entry in zip(zip(d['sup_id'], d['adm_lb_nom']),
                                d['emr_lb_systeme'] + '\t' + d['list_azimut']):
                processor.new_azimuth_map[key].append(entry)
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