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
    """Represente un message SWIFT extrait"""
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
            logging.error(f"Erreur extraction PDF {pdf_path}: {e}")
            return ""
    
    @staticmethod
    def parse_mt910(text: str, file_path: str) -> SwiftMessage:
        """Parse un message MT910"""
        ref_pattern = r':20:(\w+)'
        amount_pattern = r':32[AB]:(\d{6})([A-Z]{3})([\d,\.]+)'
        account_pattern = r':25:(\w+)'
        trn_pattern = r':21:(\w+)'
        
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
                 no_match_dir: str = "PAS_MATCH", history_file: str = "matching_history.json",
                 start_date: str = "082025"):
        self.mt910_dir = Path(mt910_dir)
        self.pacs008_dir = Path(pacs008_dir)
        self.match_dir = Path(match_dir)
        self.no_match_dir = Path(no_match_dir)
        self.history_file = Path(history_file)
        self.parser = SwiftParser()
        
        self.start_date = self._parse_start_date(start_date)
        
        self.match_dir.mkdir(exist_ok=True)
        self.no_match_dir.mkdir(exist_ok=True)
        
        self.history = self._load_history()
        
        logging.info(f"Traitement des fichiers a partir de: {self.start_date.strftime('%B %Y')}")
    
    def _parse_start_date(self, start_date: str) -> datetime:
        """Parse la date de debut au format MMAAAA"""
        try:
            month = int(start_date[:2])
            year = int(start_date[2:])
            return datetime(year, month, 1)
        except:
            logging.warning(f"Format de date invalide: {start_date}, utilisation aout 2025 par defaut")
            return datetime(2025, 8, 1)
    
    def _is_date_in_range(self, month_str: str) -> bool:
        """Verifie si un mois_annee est dans la plage de traitement"""
        try:
            month_num = int(month_str[:2])
            year_num = 2000 + int(month_str[2:])
            file_date = datetime(year_num, month_num, 1)
            return file_date >= self.start_date
        except:
            return True
    
    def _load_history(self) -> Dict:
        """Charge l'historique des fichiers traites"""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'processed_mt910' in data:
                        data['processed_mt910'] = set(data['processed_mt910'])
                    if 'processed_pacs008' in data:
                        data['processed_pacs008'] = set(data['processed_pacs008'])
                    return data
            except Exception as e:
                logging.error(f"Erreur chargement historique: {e}")
                return self._create_empty_history()
        return self._create_empty_history()
    
    def _create_empty_history(self) -> Dict:
        """Cree une structure d'historique vide"""
        return {
            'matched_files': {},
            'processed_mt910': set(),
            'processed_pacs008': set(),
            'pending_mt910': {},
            'pending_pacs008': {},
            'waiting_days': 5,
            'last_run': None
        }
    
    def _save_history(self):
        """Sauvegarde l'historique"""
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
            logging.error(f"Erreur sauvegarde historique: {e}")
    
    def _get_file_hash(self, file_path: str) -> str:
        """Calcule le hash SHA256 d'un fichier"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception as e:
            logging.warning(f"Erreur calcul hash pour {file_path}: {e}")
            return ""
    
    def _is_file_processed(self, file_path: str, message_type: str) -> bool:
        """Verifie si un fichier a deja ete traite"""
        file_hash = self._get_file_hash(file_path)
        if not file_hash:
            return False
        
        if message_type == "MT910":
            return file_hash in self.history['processed_mt910']
        else:
            return file_hash in self.history['processed_pacs008']
    
    def _mark_file_processed(self, file_path: str, message_type: str):
        """Marque un fichier comme traite"""
        file_hash = self._get_file_hash(file_path)
        if not file_hash:
            return
        
        if message_type == "MT910":
            self.history['processed_mt910'].add(file_hash)
        else:
            self.history['processed_pacs008'].add(file_hash)
    
    def _is_already_matched(self, mt910_path: str, pacs008_path: str) -> bool:
        """Verifie si une paire de fichiers a deja ete matchee"""
        mt910_hash = self._get_file_hash(mt910_path)
        pacs008_hash = self._get_file_hash(pacs008_path)
        
        if not mt910_hash or not pacs008_hash:
            return False
        
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
        
        self._mark_file_processed(mt910_path, "MT910")
        self._mark_file_processed(pacs008_path, "PACS008")
        
        if mt910_hash in self.history['pending_mt910']:
            del self.history['pending_mt910'][mt910_hash]
        if pacs008_hash in self.history['pending_pacs008']:
            del self.history['pending_pacs008'][pacs008_hash]
    
    def _add_to_pending(self, file_path: str, message_type: str):
        """Ajoute un fichier non-matche a la liste d'attente"""
        file_hash = self._get_file_hash(file_path)
        if not file_hash:
            return
        
        pending_dict = (self.history['pending_mt910'] if message_type == "MT910" 
                       else self.history['pending_pacs008'])
        
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
        
        for file_hash, data in list(self.history['pending_mt910'].items()):
            first_seen = datetime.fromisoformat(data['first_seen'])
            days_waiting = (now - first_seen).days
            
            if days_waiting >= waiting_days:
                expired_mt910.append({
                    'hash': file_hash,
                    'path': data['path'],
                    'days_waiting': days_waiting
                })
        
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
        """Marque un fichier comme definitivement non-matche"""
        if message_type == "MT910":
            self.history['processed_mt910'].add(file_hash)
            if file_hash in self.history['pending_mt910']:
                del self.history['pending_mt910'][file_hash]
        else:
            self.history['processed_pacs008'].add(file_hash)
            if file_hash in self.history['pending_pacs008']:
                del self.history['pending_pacs008'][file_hash]
    
    def scan_directory(self, base_dir: Path, message_type: str, full_scan: bool = False) -> List[SwiftMessage]:
        """Scanne un repertoire et extrait tous les messages NON traites"""
        messages = []
        new_files_count = 0
        skipped_files_count = 0
        skipped_old_files = 0
        
        if full_scan or self.history['last_run'] is None:
            cutoff_date = None
            logging.info(f"   Scan complet de tous les repertoires...")
        else:
            cutoff_date = datetime.now() - timedelta(days=10)
            logging.debug(f"   Scan optimise (derniers 10 jours)")
        
        if message_type == "MT910":
            swift_sgci_dir = base_dir / "swift_sgci"
            if not swift_sgci_dir.exists():
                logging.warning(f"Repertoire swift_sgci introuvable: {swift_sgci_dir}")
                logging.warning(f"Verifiez que MT910_DIR pointe vers le parent de swift_sgci")
                return messages
            base_scan_dir = swift_sgci_dir
            subpath_pattern = ["entrant", "mt910"]
            use_wildcard = False
        else:
            base_scan_dir = base_dir
            subpath_pattern = ["entrant", "pacs.008"]
            use_wildcard = True
        
        logging.info(f"   Repertoire de base pour {message_type}: {base_scan_dir}")
        
        if not base_scan_dir.exists():
            logging.error(f"Le repertoire de base n'existe pas: {base_scan_dir}")
            return messages
        
        for month_dir in base_scan_dir.iterdir():
            if not month_dir.is_dir():
                continue
            
            if not re.match(r'^\d{4}$', month_dir.name):
                continue
            
            if not self._is_date_in_range(month_dir.name):
                skipped_old_files += 1
                continue
            
            if cutoff_date:
                try:
                    month_str = month_dir.name
                    month_num = int(month_str[:2])
                    year_num = 2000 + int(month_str[2:])
                    
                    if datetime(year_num, month_num, 1) < cutoff_date.replace(day=1):
                        continue
                except:
                    pass
            
            logging.debug(f"   Traitement du mois: {month_dir.name}")
            
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                
                if not re.match(r'^\d{6}$', day_dir.name):
                    continue
                
                if cutoff_date:
                    try:
                        dir_mtime = datetime.fromtimestamp(day_dir.stat().st_mtime)
                        if dir_mtime < cutoff_date:
                            continue
                    except:
                        pass
                
                target_dir = day_dir
                path_valid = True
                
                for subdir in subpath_pattern:
                    target_dir = target_dir / subdir
                    if not target_dir.exists():
                        logging.debug(f"   Chemin introuvable: {target_dir}")
                        path_valid = False
                        break
                
                if not path_valid:
                    continue
                
                if message_type == "MT910":
                    logging.debug(f"   Repertoire cible MT910: {target_dir}")
                    pdf_files = list(target_dir.glob("*.pdf"))
                    logging.debug(f"   {len(pdf_files)} fichiers PDF trouves")
                    
                    for pdf_file in pdf_files:
                        if self._is_file_processed(str(pdf_file), message_type):
                            skipped_files_count += 1
                            continue
                        
                        try:
                            text = self.parser.extract_text_from_pdf(str(pdf_file))
                            if text:
                                msg = self.parser.parse_mt910(text, str(pdf_file))
                                messages.append(msg)
                                new_files_count += 1
                                logging.debug(f"   Fichier traite: {pdf_file.name}")
                        except Exception as e:
                            logging.warning(f"Erreur lecture {pdf_file.name}: {e}")
                
                else:
                    logging.debug(f"   Repertoire pacs.008: {target_dir}")
                    
                    for manu_dir in target_dir.rglob("manu"):
                        sgci_dir = manu_dir / "sgci"
                        if sgci_dir.exists() and sgci_dir.is_dir():
                            logging.debug(f"   Repertoire sgci trouve: {sgci_dir}")
                            pdf_files = list(sgci_dir.glob("*.pdf"))
                            logging.debug(f"   {len(pdf_files)} fichiers PDF dans {sgci_dir}")
                            
                            for pdf_file in pdf_files:
                                if self._is_file_processed(str(pdf_file), message_type):
                                    skipped_files_count += 1
                                    continue
                                
                                try:
                                    text = self.parser.extract_text_from_pdf(str(pdf_file))
                                    if text:
                                        msg = self.parser.parse_pacs008(text, str(pdf_file))
                                        messages.append(msg)
                                        new_files_count += 1
                                        logging.debug(f"   Fichier traite: {pdf_file.name}")
                                    else:
                                        logging.warning(f"   Fichier PDF vide: {pdf_file.name}")
                                except Exception as e:
                                    logging.warning(f"Erreur lecture {pdf_file.name}: {e}")
        
        if skipped_old_files > 0:
            logging.info(f"   {skipped_old_files} mois anterieurs a {self.start_date.strftime('%m/%Y')} ignores")
        if skipped_files_count > 0:
            logging.debug(f"   {skipped_files_count} fichiers deja traites ignores")
        if new_files_count > 0:
            logging.info(f"   {new_files_count} nouveaux fichiers {message_type} a traiter")
        else:
            logging.warning(f"   Aucun fichier {message_type} trouve dans {base_scan_dir}")
        
        return messages
    
    def match_messages(self, mt910_msgs: List[SwiftMessage], 
                      pacs008_msgs: List[SwiftMessage]) -> Tuple[List, List, List]:
        """Fait le matching entre MT910 et PACS.008"""
        matches = []
        mt910_matched = set()
        pacs008_matched = set()
        
        all_mt910 = list(mt910_msgs)
        all_pacs008 = list(pacs008_msgs)
        
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
        
        for i, mt910 in enumerate(all_mt910):
            for j, pacs008 in enumerate(all_pacs008):
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
                    self._record_match(mt910.file_path, pacs008.file_path)
                    mt910_matched.add(i)
                    pacs008_matched.add(j)
                    break
        
        mt910_non_matches = [msg for i, msg in enumerate(all_mt910) if i not in mt910_matched]
        pacs008_non_matches = [msg for i, msg in enumerate(all_pacs008) if i not in pacs008_matched]
        
        for msg in mt910_non_matches:
            if msg in mt910_msgs:
                self._add_to_pending(msg.file_path, "MT910")
        
        for msg in pacs008_non_matches:
            if msg in pacs008_msgs:
                self._add_to_pending(msg.file_path, "PACS008")
        
        return matches, mt910_non_matches, pacs008_non_matches
    
    def _is_match(self, mt910: SwiftMessage, pacs008: SwiftMessage) -> bool:
        """Verifie si deux messages correspondent"""
        if mt910.transaction_ref and pacs008.transaction_ref:
            if mt910.transaction_ref == pacs008.transaction_ref:
                return True
        
        amount_match = abs(mt910.amount - pacs008.amount) < 0.01
        date_match = mt910.date == pacs008.date
        account_match = (mt910.debit_account == pacs008.debit_account or 
                        mt910.credit_account == pacs008.credit_account)
        
        if amount_match and date_match and account_match:
            return True
        
        ref_match = mt910.reference == pacs008.reference
        if ref_match and amount_match:
            return True
        
        return False
    
    def _extract_date_structure(self, file_path: str) -> Tuple[str, str]:
        """Extrait la structure mois_annee/jour_mois_annee du chemin"""
        path_parts = Path(file_path).parts
        
        month_dir = None
        day_dir = None
        
        for i, part in enumerate(path_parts):
            if re.match(r'^\d{4}$', part) and month_dir is None:
                month_dir = part
                if i + 1 < len(path_parts) and re.match(r'^\d{6}$', path_parts[i + 1]):
                    day_dir = path_parts[i + 1]
                    break
        
        return month_dir or "", day_dir or ""
    
    def _copy_file_with_structure(self, source_path: str, dest_base: Path, message_type: str):
        """Copie un fichier en conservant la structure"""
        month_dir, day_dir = self._extract_date_structure(source_path)
        
        if not month_dir or not day_dir:
            logging.warning(f"Impossible d'extraire la structure pour: {source_path}")
            return
        
        dest_path = dest_base / message_type / month_dir / day_dir
        dest_path.mkdir(parents=True, exist_ok=True)
        
        source = Path(source_path)
        destination = dest_path / source.name
        
        try:
            shutil.copy2(source, destination)
        except Exception as e:
            logging.error(f"Erreur lors de la copie de {source_path}: {e}")
    
    def copy_matched_files(self, matches: List[Dict]):
        """Copie les fichiers matches dans le repertoire MATCH"""
        if not matches:
            return
        
        logging.info(f"Copie de {len(matches)} paires matchees...")
        
        for idx, match in enumerate(matches, start=1):
            mt910_month, mt910_day = self._extract_date_structure(match['mt910_file'])
            pacs008_month, pacs008_day = self._extract_date_structure(match['pacs008_file'])
            
            if not mt910_month or not mt910_day:
                logging.warning(f"Structure MT910 invalide: {match['mt910_file']}")
                continue
            
            if not pacs008_month or not pacs008_day:
                logging.warning(f"Structure PACS.008 invalide: {match['pacs008_file']}")
                continue
            
            dest_path = self.match_dir / mt910_month / mt910_day
            dest_path.mkdir(parents=True, exist_ok=True)
            
            mt910_original = Path(match['mt910_file']).name
            pacs008_original = Path(match['pacs008_file']).name
            
            mt910_new_name = f"{idx}_{mt910_day}_{mt910_original}"
            pacs008_new_name = f"{idx}_{pacs008_day}_{pacs008_original}"
            
            try:
                shutil.copy2(match['mt910_file'], dest_path / mt910_new_name)
                shutil.copy2(match['pacs008_file'], dest_path / pacs008_new_name)
            except Exception as e:
                logging.error(f"Erreur copie paire {idx}: {e}")
                continue
            
            if mt910_day != pacs008_day:
                logging.debug(f"   Paire {idx}: Decalage MT910({mt910_day}) <-> PACS.008({pacs008_day})")
        
        logging.info(f"   Paires copiees dans {self.match_dir}")
    
    def copy_unmatched_files(self, mt910_unmatched: List[SwiftMessage], 
                            pacs008_unmatched: List[SwiftMessage]):
        """Copie UNIQUEMENT les fichiers expires dans PAS_MATCH"""
        expired_mt910, expired_pacs008 = self._get_expired_pending_files()
        
        if not expired_mt910 and not expired_pacs008:
            return
        
        logging.info(f"Copie de {len(expired_mt910) + len(expired_pacs008)} fichiers expires dans PAS_MATCH...")
        
        for expired in expired_mt910:
            if Path(expired['path']).exists():
                self._copy_file_with_structure(expired['path'], self.no_match_dir, 'mt910')
                self._mark_as_permanently_unmatched(expired['hash'], "MT910")
        
        for expired in expired_pacs008:
            if Path(expired['path']).exists():
                self._copy_file_with_structure(expired['path'], self.no_match_dir, 'pacs008')
                self._mark_as_permanently_unmatched(expired['hash'], "PACS008")
        
        logging.info(f"   {len(expired_mt910)} MT910 + {len(expired_pacs008)} PACS.008 copies")
    
    def generate_statistics(self, matches: List, mt910_total: int, pacs008_total: int) -> Dict:
        """Genere les statistiques de matching"""
        matched_count = len(matches)
        mt910_unmatched = mt910_total - matched_count
        pacs008_unmatched = pacs008_total - matched_count
        
        matching_rate = (matched_count / max(mt910_total, pacs008_total) * 100) if max(mt910_total, pacs008_total) > 0 else 0
        
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
        """Execute le processus complet de matching"""
        start_time = time.time()
        
        is_first_run = self.history['last_run'] is None
        
        if is_first_run:
            full_scan = True
            logging.info("="*80)
            logging.info("PREMIERE EXECUTION - SCAN COMPLET DE TOUS LES FICHIERS")
            logging.info("="*80)
        elif verbose:
            logging.info("="*60)
            logging.info("DEMARRAGE DU MATCHING SWIFT")
            logging.info("="*60)
        
        if self.history['last_run'] and verbose:
            logging.info(f"Derniere execution: {self.history['last_run']}")
            logging.info(f"Fichiers deja matches: {len(self.history['matched_files'])}")
            logging.info(f"Fichiers en attente de match (< {self.history['waiting_days']} jours):")
            logging.info(f"     - MT910: {len(self.history['pending_mt910'])}")
            logging.info(f"     - PACS.008: {len(self.history['pending_pacs008'])}")
        
        logging.info("Scanning nouveaux fichiers...")
        mt910_messages = self.scan_directory(self.mt910_dir, "MT910", full_scan=full_scan)
        pacs008_messages = self.scan_directory(self.pacs008_dir, "PACS008", full_scan=full_scan)
        
        if len(mt910_messages) == 0 and len(pacs008_messages) == 0 and len(self.history['pending_mt910']) == 0 and len(self.history['pending_pacs008']) == 0:
            logging.info("Aucun nouveau fichier a traiter")
            return None, [], [], []
        
        logging.info("Matching en cours...")
        matches, mt910_unmatched, pacs008_unmatched = self.match_messages(mt910_messages, pacs008_messages)
        
        if matches:
            logging.info(f"   {len(matches)} nouveaux matches trouves")
        
        if verbose:
            stats = self.generate_statistics(matches, len(mt910_messages), len(pacs008_messages))
        else:
            stats = None
        
        if matches:
            self.copy_matched_files(matches)
        
        self.copy_unmatched_files(mt910_unmatched, pacs008_unmatched)
        
        self._save_history()
        
        elapsed_time = time.time() - start_time
        
        if is_first_run:
            logging.info("="*80)
            logging.info(f"PREMIERE EXECUTION TERMINEE")
            logging.info(f"   - Fichiers MT910 traites: {len(mt910_messages)}")
            logging.info(f"   - Fichiers PACS.008 traites: {len(pacs008_messages)}")
            logging.info(f"   - Matches trouves: {len(matches)}")
            logging.info(f"   - Temps d'execution: {elapsed_time:.2f}s")
            logging.info("="*80)
            logging.info("Les prochaines executions seront optimisees (scan des 10 derniers jours)")
        else:
            logging.info(f"Traitement termine en {elapsed_time:.2f}s")
        
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
            if matches:
                df_matches = pd.DataFrame(matches)
                df_matches.to_excel(output_path / f"matches_{timestamp}.xlsx", index=False)
            
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
            
            if stats:
                with open(output_path / f"statistics_{timestamp}.json", 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Rapports sauvegardes dans: {output_path}")
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde des rapports: {e}")
    
    def _print_summary(self, stats: Dict):
        """Affiche le resume des statistiques"""
        logging.info("\n" + "="*60)
        logging.info("RAPPORT DE MATCHING")
        logging.info("="*60)
        logging.info(f"Total MT910:           {stats['total_mt910']}")
        logging.info(f"Total PACS.008:        {stats['total_pacs008']}")
        logging.info(f"Messages matches:      {stats['matched']}")
        logging.info(f"MT910 non-matches:     {stats['mt910_unmatched']}")
        logging.info(f"PACS.008 non-matches:  {stats['pacs008_unmatched']}")
        logging.info(f"Taux de matching:      {stats['matching_rate']}%")
        logging.info("="*60)


def run_scheduled_matching(matcher: SwiftMatcher, output_dir: str):
    """Fonction appelee toutes les 5 minutes"""
    try:
        logging.info("\n" + "="*80)
        logging.info(f"Execution automatique - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*80)
        matcher.run_matching(output_dir, verbose=False)
    except Exception as e:
        logging.error(f"Erreur lors de l'execution: {e}", exc_info=True)


def run_daily_report(matcher: SwiftMatcher, output_dir: str):
    """Fonction pour generer un rapport detaille quotidien"""
    try:
        logging.info("\n" + "="*80)
        logging.info(f"RAPPORT QUOTIDIEN - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*80)
        matcher.run_matching(output_dir, verbose=True)
    except Exception as e:
        logging.error(f"Erreur lors du rapport quotidien: {e}", exc_info=True)


if __name__ == "__main__":
    # Configuration des chemins
    MT910_DIR = "D:/path/to/mt910_parent"
    PACS008_DIR = "E:/path/to/pacs008_parent"
    MATCH_DIR = "MATCH"
    NO_MATCH_DIR = "PAS_MATCH"
    OUTPUT_DIR = "output_matching"
    HISTORY_FILE = "matching_history.json"
    START_DATE = "082025"
    
    # Creer le matcher
    matcher = SwiftMatcher(
        MT910_DIR, 
        PACS008_DIR, 
        MATCH_DIR, 
        NO_MATCH_DIR,
        HISTORY_FILE,
        START_DATE
    )
    
    # Mode 1: Execution manuelle unique
    # stats, matches, mt910_unmatched, pacs008_unmatched = matcher.run_matching(OUTPUT_DIR, verbose=True)
    
    # Mode 2: Execution planifiee (production)
    logging.info("Demarrage du service de matching SWIFT")
    logging.info("Execution toutes les 5 minutes")
    logging.info("Rapport quotidien a 23:55")
    logging.info("")
    logging.info("Configuration des chemins:")
    logging.info(f"   MT910:    {MT910_DIR}")
    logging.info(f"             -> /swift_sgci/MMAA/JJMMAA/entrant/mt910/*.pdf")
    logging.info(f"   PACS.008: {PACS008_DIR}")
    logging.info(f"             -> /MMAA/JJMMAA/entrant/pacs.008/*/manu/sgci/*.pdf")
    logging.info(f"   MATCH:    {Path(MATCH_DIR).absolute()}")
    logging.info(f"   PAS_MATCH: {Path(NO_MATCH_DIR).absolute()}")
    logging.info("")
    logging.info("Appuyez sur Ctrl+C pour arreter")
    
    schedule.every(5).minutes.do(run_scheduled_matching, matcher, OUTPUT_DIR)
    schedule.every().day.at("23:55").do(run_daily_report, matcher, OUTPUT_DIR)
    
    logging.info("Execution initiale...")
    run_scheduled_matching(matcher, OUTPUT_DIR)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logging.info("\nArret du service de matching SWIFT")
        logging.info("Au revoir!")
