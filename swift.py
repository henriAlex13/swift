import os
import re
import shutil
import time
import schedule
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Set
import PyPDF2
import pandas as pd
from dataclasses import dataclass
import json
import hashlib
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('swift_matching.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class SwiftMessage:
    """Représente un message SWIFT extrait"""
    file_path: str
    date: str
    reference: str
    amount: float
    debit_account: str
    credit_account: str
    transaction_ref: str
    raw_text: str

class SwiftParser:
    """Parser pour extraire les informations des messages SWIFT depuis PDF"""
    
    @staticmethod
    def extract_text_from_pdf(pdf_path: str) -> str:
        """Extrait le texte d'un fichier PDF"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text()
                return text
        except Exception as e:
            print(f"Erreur lors de l'extraction du PDF {pdf_path}: {e}")
            return ""
    
    @staticmethod
    def parse_mt910(text: str, file_path: str) -> SwiftMessage:
        """Parse un message MT910"""
        # Patterns pour extraire les informations
        ref_pattern = r':20:(\w+)'
        amount_pattern = r':32[AB]:(\d{6})([A-Z]{3})([\d,\.]+)'
        account_pattern = r':25:(\w+)'
        trn_pattern = r':21:(\w+)'
        date_pattern = r':(\d{6})'
        
        reference = re.search(ref_pattern, text)
        amount = re.search(amount_pattern, text)
        account = re.search(account_pattern, text)
        trn = re.search(trn_pattern, text)
        
        return SwiftMessage(
            file_path=file_path,
            date=amount.group(1) if amount else "",
            reference=reference.group(1) if reference else "",
            amount=float(amount.group(3).replace(',', '.')) if amount else 0.0,
            debit_account=account.group(1) if account else "",
            credit_account="",
            transaction_ref=trn.group(1) if trn else "",
            raw_text=text
        )
    
    @staticmethod
    def parse_pacs008(text: str, file_path: str) -> SwiftMessage:
        """Parse un message PACS.008"""
        # Patterns XML pour PACS.008
        ref_pattern = r'<MsgId>(.*?)</MsgId>'
        amount_pattern = r'<InstdAmt[^>]*>([\d\.]+)</InstdAmt>'
        debtor_pattern = r'<DbtrAcct>.*?<Id>.*?<IBAN>(.*?)</IBAN>'
        creditor_pattern = r'<CdtrAcct>.*?<Id>.*?<IBAN>(.*?)</IBAN>'
        trn_pattern = r'<EndToEndId>(.*?)</EndToEndId>'
        date_pattern = r'<CreDtTm>(.*?)</CreDtTm>'
        
        reference = re.search(ref_pattern, text)
        amount = re.search(amount_pattern, text)
        debtor = re.search(debtor_pattern, text, re.DOTALL)
        creditor = re.search(creditor_pattern, text, re.DOTALL)
        trn = re.search(trn_pattern, text)
        date = re.search(date_pattern, text)
        
        return SwiftMessage(
            file_path=file_path,
            date=date.group(1)[:8] if date else "",
            reference=reference.group(1) if reference else "",
            amount=float(amount.group(1)) if amount else 0.0,
            debit_account=debtor.group(1) if debtor else "",
            credit_account=creditor.group(1) if creditor else "",
            transaction_ref=trn.group(1) if trn else "",
            raw_text=text
        )

class SwiftMatcher:
    """Classe principale pour le matching des flux SWIFT"""
    
    def __init__(self, mt910_dir: str, pacs008_dir: str, match_dir: str = "MATCH", 
                 no_match_dir: str = "PAS_MATCH", history_file: str = "matching_history.json"):
        self.mt910_dir = Path(mt910_dir)
        self.pacs008_dir = Path(pacs008_dir)
        self.match_dir = Path(match_dir)
        self.no_match_dir = Path(no_match_dir)
        self.history_file = Path(history_file)
        self.parser = SwiftParser()
        
        # Créer les répertoires de sortie
        self.match_dir.mkdir(exist_ok=True)
        self.no_match_dir.mkdir(exist_ok=True)
        
        # Charger l'historique des fichiers traités
        self.history = self._load_history()
    
    def _load_history(self) -> Dict:
        """Charge l'historique des fichiers traités"""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️  Erreur lors du chargement de l'historique: {e}")
                return self._create_empty_history()
        return self._create_empty_history()
    
    def _create_empty_history(self) -> Dict:
        """Crée une structure d'historique vide"""
        return {
            'matched_files': {},  # {file_hash: {'mt910': path, 'pacs008': path, 'match_date': date}}
            'processed_mt910': set(),  # Ensemble des hash de fichiers MT910 traités
            'processed_pacs008': set(),  # Ensemble des hash de fichiers PACS.008 traités
            'pending_mt910': {},  # {file_hash: {'path': path, 'first_seen': date}}
            'pending_pacs008': {},  # {file_hash: {'path': path, 'first_seen': date}}
            'waiting_days': 5,  # Nombre de jours d'attente avant de copier dans PAS_MATCH
            'last_run': None
        }
    
    def _save_history(self):
        """Sauvegarde l'historique"""
        # Convertir les sets en listes pour la sérialisation JSON
        history_to_save = {
            'matched_files': self.history['matched_files'],
            'processed_mt910': list(self.history['processed_mt910']),
            'processed_pacs008': list(self.history['processed_pacs008']),
            'pending_mt910': self.history['pending_mt910'],
            'pending_pacs008': self.history['pending_pacs008'],
            'waiting_days': self.history['waiting_days'],
            'last_run': datetime.now().isoformat()
        }
        
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history_to_save, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde de l'historique: {e}")
    
    def _get_file_hash(self, file_path: str) -> str:
        """Calcule le hash SHA256 d'un fichier pour l'identifier de manière unique"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception as e:
            print(f"⚠️  Erreur lors du calcul du hash pour {file_path}: {e}")
            return ""
    
    def _is_file_processed(self, file_path: str, message_type: str) -> bool:
        """Vérifie si un fichier a déjà été traité (matché ou définitivement classé)"""
        file_hash = self._get_file_hash(file_path)
        if not file_hash:
            return False
        
        # Vérifier si déjà matché
        if message_type == "MT910":
            return file_hash in self.history['processed_mt910']
        else:
            return file_hash in self.history['processed_pacs008']
    
    def _mark_file_processed(self, file_path: str, message_type: str):
        """Marque un fichier comme traité"""
        file_hash = self._get_file_hash(file_path)
        if not file_hash:
            return
        
        if message_type == "MT910":
            self.history['processed_mt910'].add(file_hash)
        else:
            self.history['processed_pacs008'].add(file_hash)
    
    def _is_already_matched(self, mt910_path: str, pacs008_path: str) -> bool:
        """Vérifie si une paire de fichiers a déjà été matchée"""
        mt910_hash = self._get_file_hash(mt910_path)
        pacs008_hash = self._get_file_hash(pacs008_path)
        
        if not mt910_hash or not pacs008_hash:
            return False
        
        # Vérifier si cette combinaison existe déjà
        match_key = f"{mt910_hash}_{pacs008_hash}"
        return match_key in self.history['matched_files']
    
    def _record_match(self, mt910_path: str, pacs008_path: str):
        """Enregistre un match dans l'historique"""
        mt910_hash = self._get_file_hash(mt910_path)
        pacs008_hash = self._get_file_hash(pacs008_path)
        
        if not mt910_hash or not pacs008_hash:
            return
        
        match_key = f"{mt910_hash}_{pacs008_hash}"
        self.history['matched_files'][match_key] = {
            'mt910': mt910_path,
            'pacs008': pacs008_path,
            'match_date': datetime.now().isoformat()
        }
        
        # Marquer les fichiers comme traités
        self._mark_file_processed(mt910_path, "MT910")
        self._mark_file_processed(pacs008_path, "PACS008")
        
        # Retirer des fichiers en attente s'ils y étaient
        if mt910_hash in self.history['pending_mt910']:
            del self.history['pending_mt910'][mt910_hash]
        if pacs008_hash in self.history['pending_pacs008']:
            del self.history['pending_pacs008'][pacs008_hash]
    
    def _add_to_pending(self, file_path: str, message_type: str):
        """Ajoute un fichier non-matché à la liste d'attente"""
        file_hash = self._get_file_hash(file_path)
        if not file_hash:
            return
        
        pending_dict = (self.history['pending_mt910'] if message_type == "MT910" 
                       else self.history['pending_pacs008'])
        
        # N'ajouter que s'il n'existe pas déjà
        if file_hash not in pending_dict:
            pending_dict[file_hash] = {
                'path': file_path,
                'first_seen': datetime.now().isoformat()
            }
    
    def _get_expired_pending_files(self) -> Tuple[List[Dict], List[Dict]]:
        """Retourne les fichiers en attente depuis plus de waiting_days jours"""
        now = datetime.now()
        waiting_days = self.history['waiting_days']
        
        expired_mt910 = []
        expired_pacs008 = []
        
        # Vérifier MT910
        for file_hash, data in list(self.history['pending_mt910'].items()):
            first_seen = datetime.fromisoformat(data['first_seen'])
            days_waiting = (now - first_seen).days
            
            if days_waiting >= waiting_days:
                expired_mt910.append({
                    'hash': file_hash,
                    'path': data['path'],
                    'days_waiting': days_waiting
                })
        
        # Vérifier PACS.008
        for file_hash, data in list(self.history['pending_pacs008'].items()):
            first_seen = datetime.fromisoformat(data['first_seen'])
            days_waiting = (now - first_seen).days
            
            if days_waiting >= waiting_days:
                expired_pacs008.append({
                    'hash': file_hash,
                    'path': data['path'],
                    'days_waiting': days_waiting
                })
        
        return expired_mt910, expired_pacs008
    
    def _mark_as_permanently_unmatched(self, file_hash: str, message_type: str):
        """Marque un fichier comme définitivement non-matché après expiration"""
        if message_type == "MT910":
            self.history['processed_mt910'].add(file_hash)
            if file_hash in self.history['pending_mt910']:
                del self.history['pending_mt910'][file_hash]
        else:
            self.history['processed_pacs008'].add(file_hash)
            if file_hash in self.history['pending_pacs008']:
                del self.history['pending_pacs008'][file_hash]
        
    def scan_directory(self, base_dir: Path, message_type: str, full_scan: bool = False) -> List[SwiftMessage]:
        """Scanne un répertoire et extrait tous les messages NON traités (optimisé)"""
        messages = []
        new_files_count = 0
        skipped_files_count = 0
        
        # Déterminer si on doit faire un scan complet ou optimisé
        if full_scan or self.history['last_run'] is None:
            # Premier scan ou scan complet : traiter tous les fichiers
            cutoff_date = None
            logging.info(f"   📂 Scan complet de tous les répertoires...")
        else:
            # Scan optimisé : seulement les 10 derniers jours
            cutoff_date = datetime.now() - timedelta(days=10)
            logging.debug(f"   📂 Scan optimisé (derniers 10 jours)")
        
        # Parcourir l'arborescence mois_annee/jour_mois_annee
        for month_dir in base_dir.iterdir():
            if not month_dir.is_dir():
                continue
            
            # Filtrer les mois récents si cutoff_date défini
            if cutoff_date:
                try:
                    month_str = month_dir.name  # ex: 0125
                    month_num = int(month_str[:2])
                    year_num = 2000 + int(month_str[2:])
                    
                    # Si le mois est trop ancien, skip
                    if datetime(year_num, month_num, 1) < cutoff_date.replace(day=1):
                        continue
                except:
                    pass  # Si parsing échoue, on traite quand même
                
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                
                # Optimisation: Vérifier la date de modification du répertoire (seulement en mode optimisé)
                if cutoff_date:
                    dir_mtime = datetime.fromtimestamp(day_dir.stat().st_mtime)
                    if dir_mtime < cutoff_date:
                        continue
                    
                # Traiter tous les fichiers PDF du jour
                for pdf_file in day_dir.glob("*.pdf"):
                    # Vérifier si le fichier a déjà été traité
                    if self._is_file_processed(str(pdf_file), message_type):
                        skipped_files_count += 1
                        continue
                    
                    text = self.parser.extract_text_from_pdf(str(pdf_file))
                    if text:
                        if message_type == "MT910":
                            msg = self.parser.parse_mt910(text, str(pdf_file))
                        else:
                            msg = self.parser.parse_pacs008(text, str(pdf_file))
                        messages.append(msg)
                        new_files_count += 1
        
        if skipped_files_count > 0:
            logging.debug(f"   ⏭️  {skipped_files_count} fichiers déjà traités ignorés")
        if new_files_count > 0:
            logging.info(f"   🆕 {new_files_count} nouveaux fichiers {message_type} à traiter")
                        
        return messages
    
    def match_messages(self, mt910_msgs: List[SwiftMessage], 
                      pacs008_msgs: List[SwiftMessage]) -> Tuple[List, List, List]:
        """
        Fait le matching entre MT910 et PACS.008
        Retourne: (matches, mt910_non_matches, pacs008_non_matches)
        """
        matches = []
        mt910_matched = set()
        pacs008_matched = set()
        
        # Charger aussi les fichiers en attente pour tenter de les matcher
        all_mt910 = list(mt910_msgs)
        all_pacs008 = list(pacs008_msgs)
        
        # Ajouter les fichiers en attente qui ne sont pas encore expirés
        for file_hash, data in self.history['pending_mt910'].items():
            if Path(data['path']).exists():
                try:
                    text = self.parser.extract_text_from_pdf(data['path'])
                    if text:
                        msg = self.parser.parse_mt910(text, data['path'])
                        all_mt910.append(msg)
                except Exception as e:
                    logging.warning(f"Erreur lecture fichier en attente {data['path']}: {e}")
        
        for file_hash, data in self.history['pending_pacs008'].items():
            if Path(data['path']).exists():
                try:
                    text = self.parser.extract_text_from_pdf(data['path'])
                    if text:
                        msg = self.parser.parse_pacs008(text, data['path'])
                        all_pacs008.append(msg)
                except Exception as e:
                    logging.warning(f"Erreur lecture fichier en attente {data['path']}: {e}")
        
        # Effectuer le matching
        for i, mt910 in enumerate(all_mt910):
            for j, pacs008 in enumerate(all_pacs008):
                # Vérifier si cette paire n'a pas déjà été matchée
                if self._is_already_matched(mt910.file_path, pacs008.file_path):
                    continue
                
                if self._is_match(mt910, pacs008):
                    match_data = {
                        'mt910_file': mt910.file_path,
                        'pacs008_file': pacs008.file_path,
                        'reference': mt910.reference,
                        'transaction_ref': mt910.transaction_ref,
                        'amount': mt910.amount,
                        'date': mt910.date,
                        'debit_account': mt910.debit_account or pacs008.debit_account,
                        'credit_account': pacs008.credit_account
                    }
                    matches.append(match_data)
                    
                    # Enregistrer le match dans l'historique
                    self._record_match(mt910.file_path, pacs008.file_path)
                    
                    mt910_matched.add(i)
                    pacs008_matched.add(j)
                    break
        
        # Les fichiers non-matchés sont ajoutés à la liste d'attente
        mt910_non_matches = [msg for i, msg in enumerate(all_mt910) if i not in mt910_matched]
        pacs008_non_matches = [msg for i, msg in enumerate(all_pacs008) if i not in pacs008_matched]
        
        # Ajouter les nouveaux non-matchés à la liste d'attente
        for msg in mt910_non_matches:
            if msg in mt910_msgs:  # Seulement les nouveaux fichiers
                self._add_to_pending(msg.file_path, "MT910")
        
        for msg in pacs008_non_matches:
            if msg in pacs008_msgs:  # Seulement les nouveaux fichiers
                self._add_to_pending(msg.file_path, "PACS008")
        
        return matches, mt910_non_matches, pacs008_non_matches
    
    def _extract_date_structure(self, file_path: str) -> Tuple[str, str]:
        """
        Extrait la structure mois_annee/jour_mois_annee du chemin
        Exemple: /path/to/0125/010125/file.pdf -> ('0125', '010125')
        """
        path_parts = Path(file_path).parts
        # Récupérer les deux derniers répertoires avant le fichier
        if len(path_parts) >= 3:
            day_dir = path_parts[-2]  # ex: 010125
            month_dir = path_parts[-3]  # ex: 0125
            return month_dir, day_dir
        return "", ""
    
    def _copy_file_with_structure(self, source_path: str, dest_base: Path, 
                                   message_type: str):
        """
        Copie un fichier en conservant la structure mois_annee/jour_mois_annee
        """
        month_dir, day_dir = self._extract_date_structure(source_path)
        
        if not month_dir or not day_dir:
            print(f"⚠️  Impossible d'extraire la structure pour: {source_path}")
            return
        
        # Créer la structure: MATCH/mt910/0125/010125/ ou PAS_MATCH/pacs008/0125/010125/
        dest_path = dest_base / message_type / month_dir / day_dir
        dest_path.mkdir(parents=True, exist_ok=True)
        
        # Copier le fichier
        source = Path(source_path)
        destination = dest_path / source.name
        
        try:
            shutil.copy2(source, destination)
        except Exception as e:
            print(f"❌ Erreur lors de la copie de {source_path}: {e}")
    
    def copy_matched_files(self, matches: List[Dict]):
        """Copie les fichiers matchés dans le répertoire MATCH avec clé unique"""
        print("\n📁 Copie des fichiers matchés...")
        
        for idx, match in enumerate(matches, start=1):
            # Extraire les structures de date des deux fichiers
            mt910_month, mt910_day = self._extract_date_structure(match['mt910_file'])
            pacs008_month, pacs008_day = self._extract_date_structure(match['pacs008_file'])
            
            if not mt910_month or not mt910_day:
                print(f"⚠️  Impossible d'extraire la structure MT910 pour: {match['mt910_file']}")
                continue
            
            if not pacs008_month or not pacs008_day:
                print(f"⚠️  Impossible d'extraire la structure PACS.008 pour: {match['pacs008_file']}")
                continue
            
            # Utiliser la date du MT910 comme référence pour le répertoire de destination
            # (vous pouvez changer cette logique si vous préférez PACS.008 ou une autre règle)
            dest_path = self.match_dir / mt910_month / mt910_day
            dest_path.mkdir(parents=True, exist_ok=True)
            
            # Extraire les noms de fichiers originaux
            mt910_original = Path(match['mt910_file']).name
            pacs008_original = Path(match['pacs008_file']).name
            
            # Créer les nouveaux noms avec:
            # - la clé unique
            # - la date d'origine du fichier
            # - le nom original
            mt910_new_name = f"{idx}_{mt910_day}_{mt910_original}"
            pacs008_new_name = f"{idx}_{pacs008_day}_{pacs008_original}"
            
            # Copier les fichiers avec les nouveaux noms
            try:
                shutil.copy2(match['mt910_file'], dest_path / mt910_new_name)
                shutil.copy2(match['pacs008_file'], dest_path / pacs008_new_name)
            except Exception as e:
                print(f"❌ Erreur lors de la copie de la paire {idx}: {e}")
                continue
            
            # Log si les dates sont différentes
            if mt910_day != pacs008_day:
                print(f"   ℹ️  Paire {idx}: MT910 ({mt910_day}) ↔ PACS.008 ({pacs008_day}) - Décalage de dates détecté")
        
        print(f"   ✓ {len(matches)} paires de fichiers copiés dans {self.match_dir}")
    
    def copy_unmatched_files(self, mt910_unmatched: List[SwiftMessage], 
                            pacs008_unmatched: List[SwiftMessage]):
        """Copie UNIQUEMENT les fichiers expirés (> waiting_days jours) dans PAS_MATCH"""
        
        # Récupérer les fichiers qui ont dépassé le délai d'attente
        expired_mt910, expired_pacs008 = self._get_expired_pending_files()
        
        if not expired_mt910 and not expired_pacs008:
            return
        
        logging.info(f"📁 Copie de {len(expired_mt910) + len(expired_pacs008)} fichiers expirés dans PAS_MATCH...")
        
        # Copier MT910 expirés
        for expired in expired_mt910:
            if Path(expired['path']).exists():
                self._copy_file_with_structure(
                    expired['path'], 
                    self.no_match_dir, 
                    'mt910'
                )
                # Marquer comme définitivement non-matché
                self._mark_as_permanently_unmatched(expired['hash'], "MT910")
        
        # Copier PACS.008 expirés
        for expired in expired_pacs008:
            if Path(expired['path']).exists():
                self._copy_file_with_structure(
                    expired['path'], 
                    self.no_match_dir, 
                    'pacs008'
                )
                # Marquer comme définitivement non-matché
                self._mark_as_permanently_unmatched(expired['hash'], "PACS008")
        
        logging.info(f"   ✓ {len(expired_mt910)} MT910 + {len(expired_pacs008)} PACS.008 copiés")
    
        """Vérifie si deux messages correspondent selon les critères"""
        # Critère 1: Référence de transaction
        if mt910.transaction_ref and pacs008.transaction_ref:
            if mt910.transaction_ref == pacs008.transaction_ref:
                return True
        
        # Critère 2: Montant + Date + Compte
        amount_match = abs(mt910.amount - pacs008.amount) < 0.01
        date_match = mt910.date == pacs008.date
        account_match = (mt910.debit_account == pacs008.debit_account or 
                        mt910.credit_account == pacs008.credit_account)
        
        if amount_match and date_match and account_match:
            return True
        
        # Critère 3: Référence + Montant
        ref_match = mt910.reference == pacs008.reference
        if ref_match and amount_match:
            return True
        
        return False
    
    def generate_statistics(self, matches: List, mt910_total: int, 
                          pacs008_total: int) -> Dict:
        """Génère les statistiques de matching"""
        matched_count = len(matches)
        mt910_unmatched = mt910_total - matched_count
        pacs008_unmatched = pacs008_total - matched_count
        
        # Taux de matching
        matching_rate = (matched_count / max(mt910_total, pacs008_total) * 100) if max(mt910_total, pacs008_total) > 0 else 0
        
        # Volume par jour
        daily_volumes = {}
        for match in matches:
            date = match['date']
            if date not in daily_volumes:
                daily_volumes[date] = 0
            daily_volumes[date] += 1
        
        stats = {
            'total_mt910': mt910_total,
            'total_pacs008': pacs008_total,
            'matched': matched_count,
            'mt910_unmatched': mt910_unmatched,
            'pacs008_unmatched': pacs008_unmatched,
            'matching_rate': round(matching_rate, 2),
            'daily_volumes': daily_volumes
        }
        
        return stats
    
    def run_matching(self, output_dir: str = "output", verbose: bool = False, full_scan: bool = False):
        """Exécute le processus complet de matching (optimisé pour exécution fréquente)"""
        start_time = time.time()
        
        # Déterminer si c'est la première exécution
        is_first_run = self.history['last_run'] is None
        
        if is_first_run:
            full_scan = True  # Force le scan complet pour la première exécution
            logging.info("="*80)
            logging.info("🚀 PREMIÈRE EXÉCUTION - SCAN COMPLET DE TOUS LES FICHIERS")
            logging.info("="*80)
        elif verbose:
            logging.info("="*60)
            logging.info("🚀 DÉMARRAGE DU MATCHING SWIFT")
            logging.info("="*60)
        
        if self.history['last_run'] and verbose:
            logging.info(f"ℹ️  Dernière exécution: {self.history['last_run']}")
            logging.info(f"ℹ️  Fichiers déjà matchés: {len(self.history['matched_files'])}")
            logging.info(f"ℹ️  Fichiers en attente de match (< {self.history['waiting_days']} jours):")
            logging.info(f"     - MT910: {len(self.history['pending_mt910'])}")
            logging.info(f"     - PACS.008: {len(self.history['pending_pacs008'])}")
        
        # Scanner les nouveaux fichiers
        logging.info("🔍 Scanning nouveaux fichiers...")
        mt910_messages = self.scan_directory(self.mt910_dir, "MT910", full_scan=full_scan)
        pacs008_messages = self.scan_directory(self.pacs008_dir, "PACS008", full_scan=full_scan)
        
        if len(mt910_messages) == 0 and len(pacs008_messages) == 0 and len(self.history['pending_mt910']) == 0 and len(self.history['pending_pacs008']) == 0:
            logging.info("✅ Aucun nouveau fichier à traiter")
            return None, [], [], []
        
        # Matching
        logging.info("🔗 Matching en cours...")
        matches, mt910_unmatched, pacs008_unmatched = self.match_messages(
            mt910_messages, pacs008_messages
        )
        
        if matches:
            logging.info(f"   ✓ {len(matches)} nouveaux matches trouvés")
        
        # Générer les statistiques uniquement si verbose
        if verbose:
            stats = self.generate_statistics(matches, len(mt910_messages), len(pacs008_messages))
        else:
            stats = None
        
        # Copier les fichiers
        if matches:
            self.copy_matched_files(matches)
        
        # Copier SEULEMENT les fichiers expirés dans PAS_MATCH
        self.copy_unmatched_files(mt910_unmatched, pacs008_unmatched)
        
        # Sauvegarder l'historique après traitement
        self._save_history()
        
        elapsed_time = time.time() - start_time
        
        if is_first_run:
            logging.info("="*80)
            logging.info(f"✅ PREMIÈRE EXÉCUTION TERMINÉE")
            logging.info(f"   - Fichiers MT910 traités: {len(mt910_messages)}")
            logging.info(f"   - Fichiers PACS.008 traités: {len(pacs008_messages)}")
            logging.info(f"   - Matches trouvés: {len(matches)}")
            logging.info(f"   - Temps d'exécution: {elapsed_time:.2f}s")
            logging.info("="*80)
            logging.info("ℹ️  Les prochaines exécutions seront optimisées (scan des 10 derniers jours)")
        else:
            logging.info(f"⏱️  Traitement terminé en {elapsed_time:.2f}s")
        
        # Sauvegarder les rapports seulement si des matches ont été trouvés
        if matches and verbose:
            self._save_reports(output_dir, matches, mt910_unmatched, pacs008_unmatched, stats)
        
        if verbose and stats:
            self._print_summary(stats)
        
        return stats, matches, mt910_unmatched, pacs008_unmatched
    
    def _save_reports(self, output_dir: str, matches: List, mt910_unmatched: List, 
                     pacs008_unmatched: List, stats: Dict):
        """Sauvegarde les rapports Excel et JSON"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Matches
            if matches:
                df_matches = pd.DataFrame(matches)
                df_matches.to_excel(output_path / f"matches_{timestamp}.xlsx", index=False)
            
            # Non-matches MT910
            if mt910_unmatched:
                df_mt910_unmatched = pd.DataFrame([
                    {
                        'file': msg.file_path,
                        'reference': msg.reference,
                        'amount': msg.amount,
                        'date': msg.date,
                        'transaction_ref': msg.transaction_ref
                    }
                    for msg in mt910_unmatched
                ])
                df_mt910_unmatched.to_excel(output_path / f"mt910_unmatched_{timestamp}.xlsx", index=False)
            
            # Non-matches PACS.008
            if pacs008_unmatched:
                df_pacs008_unmatched = pd.DataFrame([
                    {
                        'file': msg.file_path,
                        'reference': msg.reference,
                        'amount': msg.amount,
                        'date': msg.date,
                        'transaction_ref': msg.transaction_ref
                    }
                    for msg in pacs008_unmatched
                ])
                df_pacs008_unmatched.to_excel(output_path / f"pacs008_unmatched_{timestamp}.xlsx", index=False)
            
            # Statistiques
            if stats:
                with open(output_path / f"statistics_{timestamp}.json", 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, ensure_ascii=False)
            
            logging.info(f"✅ Rapports sauvegardés dans: {output_path}")
        except Exception as e:
            logging.error(f"❌ Erreur lors de la sauvegarde des rapports: {e}")
    
    def _print_summary(self, stats: Dict):
        """Affiche le résumé des statistiques"""
        logging.info("\n" + "="*60)
        logging.info("📈 RAPPORT DE MATCHING")
        logging.info("="*60)
        logging.info(f"Total MT910:           {stats['total_mt910']}")
        logging.info(f"Total PACS.008:        {stats['total_pacs008']}")
        logging.info(f"Messages matchés:      {stats['matched']}")
        logging.info(f"MT910 non-matchés:     {stats['mt910_unmatched']}")
        logging.info(f"PACS.008 non-matchés:  {stats['pacs008_unmatched']}")
        logging.info(f"Taux de matching:      {stats['matching_rate']}%")
        logging.info("="*60)


# Exemple d'utilisation
if __name__ == "__main__":
    # Configuration des chemins
    MT910_DIR = "/path/to/mt910"
    PACS008_DIR = "/path/to/pacs008"
    MATCH_DIR = "MATCH"
    NO_MATCH_DIR = "PAS_MATCH"
    OUTPUT_DIR = "output_matching"
    HISTORY_FILE = "matching_history.json"
    
    # Créer le matcher et exécuter
    matcher = SwiftMatcher(
        MT910_DIR, 
        PACS008_DIR, 
        MATCH_DIR, 
        NO_MATCH_DIR,
        HISTORY_FILE
    )
    stats, matches, mt910_unmatched, pacs008_unmatched = matcher.run_matching(OUTPUT_DIR)
