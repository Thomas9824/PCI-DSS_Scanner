#!/usr/bin/env python3
"""
Script de comparaison simple entre les fichiers CSV EN (req_num, text) et la table v4_requirements_en (reqid, pci_requirement).
Vérifie si les mêmes lignes matchent entre les deux sources.
"""

import csv
import os
import glob
from datetime import datetime
from typing import Dict, List, Tuple
import mysql.connector
from dotenv import load_dotenv

class SimpleCSVBDDComparer:
    def __init__(self, db_config: Dict = None):
        load_dotenv()
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_DATABASE', 'pci_saq')
        }
        
    def find_latest_csv_file(self) -> str:
        """Trouve le fichier CSV EN le plus récent."""
        patterns = [
            "**/SAQ*EN.csv",
            "downloads/**/SAQ*EN.csv"
        ]
        
        csv_files = []
        for pattern in patterns:
            files = glob.glob(pattern, recursive=True)
            csv_files.extend(files)
            
        if not csv_files:
            return None
            
        # Retourner le plus récent
        csv_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return csv_files[0]
    
    def load_csv_requirements(self, csv_file: str) -> Dict[str, str]:
        """Charge les req_num et text depuis le CSV."""
        requirements = {}
        
        try:
            with open(csv_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    req_num = row.get('req_num', '').strip()
                    text = row.get('text', '').strip()
                    
                    if req_num and text:
                        requirements[req_num] = text
                        
        except Exception as e:
            print(f"Erreur lors du chargement de {csv_file}: {e}")
            
        return requirements
    
    def load_db_requirements(self) -> Dict[str, str]:
        """Charge les reqid et pci_requirement depuis la BDD."""
        requirements = {}
        
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            query = "SELECT reqid, pci_requirement FROM v4_requirements_en WHERE reqid IS NOT NULL AND pci_requirement IS NOT NULL AND saq_d = 1"
            cursor.execute(query)
            
            for reqid, pci_requirement in cursor.fetchall():
                if reqid and pci_requirement:
                    requirements[str(reqid).strip()] = str(pci_requirement).strip()
            
            cursor.close()
            conn.close()
            
            print(f"Données BDD chargées: {len(requirements)} requirements")
            
        except Exception as e:
            print(f"Erreur de connexion à la BDD: {e}")
            print("Utilisation de données de test...")
            requirements = self.get_test_db_data()
            
        return requirements
    
    def get_test_db_data(self) -> Dict[str, str]:
        """Données de test pour simulation."""
        return {
            "1.1": "Processes and mechanisms for installing and maintaining network security controls are defined and understood.",
            "1.1.1": "All security policies and operational procedures that are identified in Requirement 1 are documented, kept up to date, in use, and known to all affected parties.",
            "1.1.2": "Roles and responsibilities for performing activities in Requirement 1 are documented, assigned, and understood.",
            "1.2": "Network security controls (NSCs) are configured and maintained."
        }
    
    def compare_requirements(self, csv_req: Dict[str, str], db_req: Dict[str, str]) -> Dict:
        """Compare les requirements entre CSV et BDD."""
        results = {
            'total_csv': len(csv_req),
            'total_db': len(db_req),
            'matching_exact': 0,
            'matching_reqid_only': 0,
            'csv_only': [],
            'db_only': [],
            'text_differences': []
        }
        
        csv_keys = set(csv_req.keys())
        db_keys = set(db_req.keys())
        
        # Requirements uniquement dans CSV
        results['csv_only'] = list(csv_keys - db_keys)
        
        # Requirements uniquement dans BDD
        results['db_only'] = list(db_keys - csv_keys)
        
        # Comparer les requirements communs
        common_keys = csv_keys.intersection(db_keys)
        
        for req_id in common_keys:
            csv_text = csv_req[req_id]
            db_text = db_req[req_id]
            
            if csv_text == db_text:
                results['matching_exact'] += 1
            else:
                results['matching_reqid_only'] += 1
                results['text_differences'].append({
                    'req_id': req_id,
                    'csv_text': csv_text,
                    'db_text': db_text
                })
        
        return results
    
    def print_report(self, csv_file: str, results: Dict):
        """Affiche le rapport de comparaison."""
        print("\n" + "=" * 80)
        print("RAPPORT DE COMPARAISON CSV vs BDD")
        print("=" * 80)
        print(f"Fichier CSV: {csv_file}")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        print("STATISTIQUES:")
        print(f"  Total requirements CSV: {results['total_csv']}")
        print(f"  Total requirements BDD: {results['total_db']}")
        print(f"  Correspondances exactes: {results['matching_exact']}")
        print(f"  Même req_id mais texte différent: {results['matching_reqid_only']}")
        print(f"  Uniquement dans CSV: {len(results['csv_only'])}")
        print(f"  Uniquement dans BDD: {len(results['db_only'])}")
        print()
        
        if results['csv_only']:
            print("REQUIREMENTS UNIQUEMENT DANS CSV:")
            for req_id in results['csv_only']:
                print(f"  - {req_id}")
            print()
        
        if results['db_only']:
            print("REQUIREMENTS UNIQUEMENT DANS BDD:")
            for req_id in results['db_only']:
                print(f"  - {req_id}")
            print()
        
        if results['text_differences']:
            print("REQUIREMENTS AVEC TEXTE DIFFÉRENT:")
            for diff in results['text_differences']:
                print(f"  - {diff['req_id']}")
    
    def run_comparison(self):
        """Lance la comparaison simple."""
        print("Recherche du fichier CSV EN le plus récent...")
        
        csv_file = self.find_latest_csv_file()
        if not csv_file:
            print("Aucun fichier CSV EN trouvé.")
            return
        
        print(f"Fichier trouvé: {csv_file}")
        
        print("Chargement des données CSV...")
        csv_requirements = self.load_csv_requirements(csv_file)
        
        print("Chargement des données BDD...")
        db_requirements = self.load_db_requirements()
        
        print("Comparaison des données...")
        results = self.compare_requirements(csv_requirements, db_requirements)
        
        self.print_report(csv_file, results)

def main():
    """Fonction principale."""
    print("Comparaison simple CSV (req_num, text) vs BDD (reqid, pci_requirement)")
    
    comparer = SimpleCSVBDDComparer()
    comparer.run_comparison()

if __name__ == "__main__":
    main()