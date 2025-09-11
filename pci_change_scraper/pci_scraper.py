#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper pour extraire les documents PCI DSS et SAQ depuis le site PCI Security Standards
Utilise Selenium pour gérer le contenu chargé dynamiquement
"""

import time
import csv
import logging
import os
import glob
import shutil
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Set
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PCIDocumentScraper:
    """Scraper pour extraire les documents PCI DSS et SAQ"""
    
    def __init__(self, headless: bool = False):
        """
        Initialise le scraper
        
        Args:
            headless: Si True, exécute Chrome en mode headless
        """
        self.url = "https://www.pcisecuritystandards.org/document_library/"
        self.driver = None
        self.wait = None
        self.headless = headless
        self.documents = []
        
    def setup_driver(self):
        """Configure et initialise le driver Selenium"""
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Utilise webdriver-manager pour gérer automatiquement ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("Driver Selenium configuré avec succès")
            
        except Exception as e:
            logger.error(f"Erreur lors de la configuration du driver: {e}")
            raise
    
    def wait_for_page_load(self):
        """Attend que la page soit complètement chargée"""
        try:
            # Attend que le conteneur principal soit présent
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "document_name")))
            time.sleep(3)  # Temps supplémentaire pour le chargement dynamique
            logger.info("Page chargée avec succès")
        except TimeoutException:
            logger.warning("Timeout lors du chargement de la page")
    
    def select_filter(self, filter_value: str) -> bool:
        """
        Sélectionne un filtre spécifique (PCI DSS ou SAQ)
        
        Args:
            filter_value: La valeur du filtre à sélectionner ("PCI DSS" ou "SAQ")
            
        Returns:
            bool: True si la sélection a réussi, False sinon
        """
        try:
            logger.info(f"Sélection du filtre: {filter_value}")
            
            # Utilise le select natif (méthode la plus fiable)
            native_select_element = self.driver.find_element(By.CSS_SELECTOR, "#document_category")
            select = Select(native_select_element)
            
            # Vérifie si le filtre est déjà sélectionné
            current_option = select.first_selected_option.text.strip()
            if current_option == filter_value:
                logger.info(f"Le filtre {filter_value} est déjà sélectionné")
                return True
            
            # Sélectionne la nouvelle option
            select.select_by_visible_text(filter_value)
            
            # Attend que le filtre soit appliqué
            time.sleep(5)
            
            # Vérifie que le filtre a été appliqué
            new_option = select.first_selected_option.text.strip()
            if new_option == filter_value:
                logger.info(f"Filtre {filter_value} appliqué avec succès")
                return True
            else:
                logger.warning(f"Le filtre {filter_value} n'a pas été appliqué correctement")
                return False
                
        except (TimeoutException, NoSuchElementException) as e:
            logger.error(f"Erreur lors de la sélection du filtre {filter_value}: {e}")
            return False
    
    def extract_documents(self, category: str) -> List[Dict[str, str]]:
        """
        Extrait les documents pour une catégorie donnée avec détection des langues
        
        Args:
            category: La catégorie des documents ("PCI DSS", "SAQ", etc.)
            
        Returns:
            List[Dict]: Liste des documents avec leurs informations
        """
        documents = []
        
        try:
            logger.info(f"Extraction des documents pour la catégorie: {category}")
            
            # Attend que les documents soient chargés
            time.sleep(3)
            
            # Trouve les documents, versions et langues par paires
            document_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.document_name")
            version_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[id*='version_select_']")
            
            logger.info(f"Trouvé {len(document_elements)} documents et {len(version_elements)} versions")
            
            # Assure-toi qu'il y a le même nombre d'éléments
            min_count = min(len(document_elements), len(version_elements))
            
            for i in range(min_count):
                try:
                    document_name = document_elements[i].text.strip()
                    version = version_elements[i].text.strip()
                    
                    if not document_name:
                        continue
                    
                    # Détecte les langues disponibles pour ce document
                    available_languages = self.detect_available_languages(i)
                    
                    # Détermine la sous-catégorie précise (SAQ vs SAQ AOC)
                    precise_category = self.determine_precise_category(document_name, category)
                    
                    # Ajoute le document à la liste
                    document_info = {
                        'name': document_name,
                        'version': version if version else "N/A",
                        'category': precise_category,
                        'available_languages': available_languages
                    }
                    
                    documents.append(document_info)
                    logger.debug(f"Document extrait: {document_name} - Version: {version} - Catégorie: {precise_category} - Langues: {available_languages}")
                    
                except Exception as e:
                    logger.warning(f"Erreur lors de l'extraction du document {i}: {e}")
                    continue
            
            # Si il y a plus de documents que de versions, traite les documents restants
            if len(document_elements) > len(version_elements):
                for i in range(len(version_elements), len(document_elements)):
                    try:
                        document_name = document_elements[i].text.strip()
                        if document_name:
                            available_languages = self.detect_available_languages(i)
                            precise_category = self.determine_precise_category(document_name, category)
                            
                            document_info = {
                                'name': document_name,
                                'version': "N/A",
                                'category': precise_category,
                                'available_languages': available_languages
                            }
                            documents.append(document_info)
                            logger.debug(f"Document extrait (sans version): {document_name} - Catégorie: {precise_category} - Langues: {available_languages}")
                    except Exception as e:
                        logger.warning(f"Erreur lors de l'extraction du document {i}: {e}")
                        continue
            
            logger.info(f"Extraction terminée: {len(documents)} documents trouvés pour {category}")
            return documents
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des documents pour {category}: {e}")
            return documents
    
    def detect_available_languages(self, document_index: int) -> str:
        """
        Détecte les langues disponibles pour un document donné via les dropdowns de langue
        
        Args:
            document_index: Index du document dans la liste
            
        Returns:
            str: Liste des langues disponibles séparées par des virgules
        """
        try:
            # Trouve le dropdown de langue associé à ce document
            language_selects = self.driver.find_elements(By.CSS_SELECTOR, "select[data-doc_idx]")
            
            if document_index < len(language_selects):
                select_element = language_selects[document_index]
                select = Select(select_element)
                
                languages = []
                for option in select.options:
                    option_text = option.text.strip()
                    if "PDF" in option_text:
                        # Extrait le nom de la langue
                        if "English PDF" in option_text:
                            languages.append("EN")
                        elif "French PDF" in option_text:
                            languages.append("FR")
                        elif "Chinese PDF" in option_text:
                            languages.append("ZH")
                        elif "German PDF" in option_text:
                            languages.append("DE")
                        elif "Japanese PDF" in option_text:
                            languages.append("JA")
                        elif "Portuguese PDF" in option_text:
                            languages.append("PT")
                        elif "Spanish PDF" in option_text:
                            languages.append("ES")
                
                return ", ".join(languages) if languages else "EN"
            else:
                # Fallback: cherche n'importe quel select près du document
                try:
                    document_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.document_name")
                    if document_index < len(document_elements):
                        # Cherche un select dans le même conteneur parent
                        parent = document_elements[document_index].find_element(By.XPATH, "../..")
                        select_element = parent.find_element(By.CSS_SELECTOR, "select")
                        select = Select(select_element)
                        
                        languages = []
                        for option in select.options:
                            option_text = option.text.strip()
                            if "English PDF" in option_text:
                                languages.append("EN")
                            elif "French PDF" in option_text:
                                languages.append("FR")
                            # Ajoutez d'autres langues selon vos besoins
                        
                        return ", ".join(languages) if languages else "EN"
                except:
                    pass
                
                return "EN"  # Défaut à anglais si pas de dropdown trouvé
                
        except Exception as e:
            logger.debug(f"Impossible de détecter les langues pour le document {document_index}: {e}")
            return "EN"  # Défaut à anglais en cas d'erreur
    
    def determine_precise_category(self, document_name: str, base_category: str) -> str:
        """
        Détermine la catégorie précise d'un document (sépare SAQ et SAQ AOC)
        
        Args:
            document_name: Nom du document
            base_category: Catégorie de base détectée
            
        Returns:
            str: Catégorie précise
        """
        document_name_lower = document_name.lower()
        
        if base_category == "SAQ":
            # Détecte les documents AOC (Attestation of Compliance)
            if any(keyword in document_name_lower for keyword in ["aoc", "attestation of compliance", "attestation"]):
                return "SAQ AOC"
            else:
                return "SAQ"
        
        return base_category
    
    def get_available_categories(self) -> List[str]:
        """
        Récupère toutes les catégories disponibles dans le dropdown
        
        Returns:
            List[str]: Liste des catégories disponibles
        """
        try:
            select_element = self.driver.find_element(By.CSS_SELECTOR, "#document_category")
            select = Select(select_element)
            
            categories = []
            for option in select.options:
                option_text = option.text.strip()
                if option_text and option_text != "Select Category":
                    categories.append(option_text)
            
            logger.info(f"Catégories disponibles détectées: {categories}")
            return categories
            
        except Exception as e:
            logger.error(f"Erreur lors de la détection des catégories: {e}")
            # Fallback vers les catégories connues
            return ["PCI DSS", "SAQ"]
    
    def scrape_all_documents(self) -> List[Dict[str, str]]:
        """
        Scrape tous les documents pour toutes les catégories disponibles (PCI DSS, SAQ, AOC, etc.)
        
        Returns:
            List[Dict]: Liste de tous les documents extraits
        """
        all_documents = []
        
        try:
            logger.info("Début du scraping de tous les documents")
            
            # Charge la page
            self.driver.get(self.url)
            self.wait_for_page_load()
            
            # Détecte automatiquement toutes les catégories disponibles
            categories = self.get_available_categories()
            
            # Filtre pour ne garder que les catégories pertinentes
            target_categories = []
            for category in categories:
                category_lower = category.lower()
                if any(keyword in category_lower for keyword in ['pci', 'dss', 'saq', 'aoc', 'attestation']):
                    target_categories.append(category)
            
            if not target_categories:
                # Fallback vers les catégories connues
                target_categories = ["PCI DSS", "SAQ"]
                logger.warning("Aucune catégorie pertinente détectée, utilisation des catégories par défaut")
            
            logger.info(f"Catégories cibles à traiter: {target_categories}")
            
            for category in target_categories:
                logger.info(f"Traitement de la catégorie: {category}")
                
                # Sélectionne le filtre
                if self.select_filter(category):
                    # Extrait les documents
                    documents = self.extract_documents(category)
                    all_documents.extend(documents)
                    
                    logger.info(f"Documents extraits pour {category}: {len(documents)}")
                else:
                    logger.error(f"Impossible de sélectionner le filtre pour {category}")
                
                # Pause entre les catégories
                time.sleep(3)
            
            self.documents = all_documents
            logger.info(f"Scraping terminé: {len(all_documents)} documents au total")
            
            return all_documents
            
        except Exception as e:
            logger.error(f"Erreur lors du scraping: {e}")
            return all_documents
    
    def load_previous_data(self, filename: str = "pci_documents.csv") -> Optional[pd.DataFrame]:
        """
        Charge les données précédentes depuis le fichier CSV
        
        Args:
            filename: Nom du fichier CSV à charger
            
        Returns:
            DataFrame des données précédentes ou None si le fichier n'existe pas
        """
        try:
            csv_path = f"/Users/thomasmionnet/Desktop/pci-dss/scraping2/{filename}"
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, encoding='utf-8')
                logger.info(f"Données précédentes chargées depuis: {csv_path} ({len(df)} documents)")
                return df
            else:
                logger.info("Aucun fichier de données précédentes trouvé")
                return None
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données précédentes: {e}")
            return None
    
    def compare_versions(self, previous_data: Optional[pd.DataFrame]) -> Dict[str, List[Dict[str, str]]]:
        """
        Compare les données actuelles avec les données précédentes
        
        Args:
            previous_data: DataFrame des données précédentes
            
        Returns:
            Dictionnaire avec les changements détectés
        """
        changes = {
            'new_documents': [],
            'updated_versions': [],
            'removed_documents': [],
            'unchanged_documents': []
        }
        
        if previous_data is None:
            logger.info("Première exécution - tous les documents sont nouveaux")
            changes['new_documents'] = self.documents.copy()
            return changes
        
        try:
            # Convertit les données actuelles en DataFrame pour faciliter la comparaison
            current_df = pd.DataFrame(self.documents)
            
            # Crée des dictionnaires pour faciliter la comparaison
            previous_dict = {}
            for _, row in previous_data.iterrows():
                key = f"{row['name']}_{row['category']}"
                previous_dict[key] = {
                    'name': row['name'],
                    'version': row['version'],
                    'category': row['category'],
                    'available_languages': row.get('available_languages', 'EN')
                }
            
            current_dict = {}
            for doc in self.documents:
                key = f"{doc['name']}_{doc['category']}"
                current_dict[key] = doc
            
            # Détecte les nouveaux documents
            for key, doc in current_dict.items():
                if key not in previous_dict:
                    changes['new_documents'].append(doc)
                    logger.info(f"📄 Nouveau document: {doc['name']} ({doc['category']})")
            
            # Détecte les versions mises à jour et les changements de langues
            for key, doc in current_dict.items():
                if key in previous_dict:
                    prev_doc = previous_dict[key]
                    version_changed = doc['version'] != prev_doc['version']
                    languages_changed = doc.get('available_languages', 'EN') != prev_doc.get('available_languages', 'EN')
                    
                    if version_changed or languages_changed:
                        change_info = {
                            'name': doc['name'],
                            'category': doc['category'],
                            'old_version': prev_doc['version'],
                            'new_version': doc['version'],
                            'old_languages': prev_doc.get('available_languages', 'EN'),
                            'new_languages': doc.get('available_languages', 'EN')
                        }
                        changes['updated_versions'].append(change_info)
                        
                        if version_changed and languages_changed:
                            logger.info(f"Version et langues mises à jour: {doc['name']} ({doc['category']}) - {prev_doc['version']} → {doc['version']}, Langues: {prev_doc.get('available_languages', 'EN')} → {doc.get('available_languages', 'EN')}")
                        elif version_changed:
                            logger.info(f"Version mise à jour: {doc['name']} ({doc['category']}) - {prev_doc['version']} → {doc['version']}")
                        elif languages_changed:
                            logger.info(f"Langues disponibles mises à jour: {doc['name']} ({doc['category']}) - {prev_doc.get('available_languages', 'EN')} → {doc.get('available_languages', 'EN')}")
                    else:
                        changes['unchanged_documents'].append(doc)
            
            # Détecte les documents supprimés
            for key, doc in previous_dict.items():
                if key not in current_dict:
                    changes['removed_documents'].append(doc)
                    logger.info(f"Document supprimé: {doc['name']} ({doc['category']})")
            
            # Résumé des changements
            logger.info(f"\nRésumé des changements:")
            logger.info(f"  • Nouveaux documents: {len(changes['new_documents'])}")
            logger.info(f"  • Versions mises à jour: {len(changes['updated_versions'])}")
            logger.info(f"  • Documents supprimés: {len(changes['removed_documents'])}")
            logger.info(f"  • Documents inchangés: {len(changes['unchanged_documents'])}")
            
            return changes
            
        except Exception as e:
            logger.error(f"Erreur lors de la comparaison des versions: {e}")
            return changes
    
    def save_changes_report(self, changes: Dict[str, List[Dict[str, str]]], timestamp: str = None):
        """
        Sauvegarde un rapport détaillé des changements
        
        Args:
            changes: Dictionnaire des changements détectés
            timestamp: Timestamp pour le nom du fichier
        """
        try:
            if timestamp is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            report_filename = f"changes_report_{timestamp}.txt"
            report_path = f"/Users/thomasmionnet/Desktop/pci-dss/scraping2/{report_filename}"
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"=== RAPPORT DE CHANGEMENTS PCI DSS/SAQ ===\n")
                f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Nouveaux documents
                if changes['new_documents']:
                    f.write(f"📄 NOUVEAUX DOCUMENTS ({len(changes['new_documents'])}):\n")
                    f.write("-" * 50 + "\n")
                    for doc in changes['new_documents']:
                        f.write(f"• {doc['name']} ({doc['category']}) - {doc['version']}\n")
                    f.write("\n")
                
                # Versions mises à jour
                if changes['updated_versions']:
                    f.write(f"VERSIONS/LANGUES MISES À JOUR ({len(changes['updated_versions'])}):\n")
                    f.write("-" * 50 + "\n")
                    for change in changes['updated_versions']:
                        f.write(f"• {change['name']} ({change['category']})\n")
                        f.write(f"  Ancienne version: {change['old_version']}\n")
                        f.write(f"  Nouvelle version: {change['new_version']}\n")
                        if 'old_languages' in change and 'new_languages' in change:
                            if change['old_languages'] != change['new_languages']:
                                f.write(f"  Anciennes langues: {change['old_languages']}\n")
                                f.write(f"  Nouvelles langues: {change['new_languages']}\n")
                        f.write("\n")
                
                # Documents supprimés
                if changes['removed_documents']:
                    f.write(f"DOCUMENTS SUPPRIMÉS ({len(changes['removed_documents'])}):\n")
                    f.write("-" * 50 + "\n")
                    for doc in changes['removed_documents']:
                        f.write(f"• {doc['name']} ({doc['category']}) - {doc['version']}\n")
                    f.write("\n")
                
                # Résumé
                total_changes = len(changes['new_documents']) + len(changes['updated_versions']) + len(changes['removed_documents'])
                f.write(f"RÉSUMÉ:\n")
                f.write("-" * 50 + "\n")
                f.write(f"Total des changements détectés: {total_changes}\n")
                f.write(f"Documents inchangés: {len(changes['unchanged_documents'])}\n")
                f.write(f"Total des documents actuels: {len(self.documents)}\n")
            
            logger.info(f"Rapport de changements sauvegardé dans: {report_path}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du rapport: {e}")
    
    def save_to_csv(self, filename: str = "pci_documents.csv", backup_previous: bool = True):
        """
        Sauvegarde les documents extraits dans un fichier CSV
        
        Args:
            filename: Nom du fichier CSV de sortie
            backup_previous: Si True, sauvegarde l'ancien fichier avant écrasement
        """
        try:
            if not self.documents:
                logger.warning("Aucun document à sauvegarder")
                return
            
            # Utilise le répertoire courant plutôt qu'un chemin codé en dur
            csv_path = filename
            
            # Sauvegarde l'ancien fichier si demandé avec métadonnées
            if backup_previous and os.path.exists(csv_path):
                # Lit l'ancien fichier pour obtenir des métadonnées
                try:
                    old_df = pd.read_csv(csv_path, encoding='utf-8')
                    old_count = len(old_df)
                    old_updated = old_df['last_updated'].iloc[0] if 'last_updated' in old_df.columns and len(old_df) > 0 else "Unknown"
                except Exception as e:
                    old_count = "Unknown"
                    old_updated = "Unknown"
                    logger.warning(f"Impossible de lire les métadonnées de l'ancien fichier: {e}")
                
                # Crée le nom de backup avec timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_filename = f"pci_documents_backup_{timestamp}.csv"
                backup_path = backup_filename
                
                # Copie le fichier
                shutil.copy2(csv_path, backup_path)
                logger.info(f"✅ Backup créé: {backup_filename}")
                logger.info(f"   📊 Ancienne version: {old_count} documents (dernière MAJ: {old_updated})")
                logger.info(f"   📊 Nouvelle version: {len(self.documents)} documents")
            
            # Crée un DataFrame pandas
            df = pd.DataFrame(self.documents)
            
            # Ajoute un timestamp
            df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Sauvegarde en CSV
            df.to_csv(csv_path, index=False, encoding='utf-8')
            
            logger.info(f"Documents sauvegardés dans: {csv_path}")
            logger.info(f"Nombre de documents sauvegardés: {len(self.documents)}")
            
            # Affiche un aperçu des données avec statistiques détaillées
            print("\n" + "="*70)
            print("📋 APERÇU DES DONNÉES EXTRAITES")
            print("="*70)
            print(df.head(10))
            
            print(f"\n📊 STATISTIQUES GÉNÉRALES:")
            print(f"Total documents: {len(df)}")
            
            print(f"\n🏷️ RÉPARTITION PAR CATÉGORIE:")
            category_counts = df['category'].value_counts()
            for category, count in category_counts.items():
                percentage = (count / len(df)) * 100
                print(f"  • {category}: {count} documents ({percentage:.1f}%)")
            
            if 'available_languages' in df.columns:
                print(f"\n🌐 LANGUES DISPONIBLES:")
                lang_counts = df['available_languages'].value_counts()
                for languages, count in lang_counts.head(10).items():  # Top 10 pour éviter trop d'affichage
                    percentage = (count / len(df)) * 100
                    print(f"  • {languages}: {count} documents ({percentage:.1f}%)")
                
                # Analyse des langues individuelles
                all_languages = []
                for lang_combo in df['available_languages']:
                    if pd.notna(lang_combo):
                        langs = [lang.strip() for lang in str(lang_combo).split(',')]
                        all_languages.extend(langs)
                
                from collections import Counter
                lang_counter = Counter(all_languages)
                print(f"\n🗣️ COUVERTURE PAR LANGUE INDIVIDUELLE:")
                for lang, count in lang_counter.most_common():
                    percentage = (count / len(df)) * 100
                    print(f"  • {lang}: {count} documents ({percentage:.1f}%)")
            
            print("="*70)
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde: {e}")
    
    def close(self):
        """Ferme le driver Selenium"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver fermé")

def main():
    """Fonction principale avec comparaison des versions"""
    scraper = None
    
    try:
        # Crée le scraper
        scraper = PCIDocumentScraper(headless=False)  # Mode visible pour le debug
        
        # Charge les données précédentes avant de commencer
        print("Chargement des données précédentes...")
        previous_data = scraper.load_previous_data("pci_documents.csv")
        
        # Configure le driver
        scraper.setup_driver()
        
        # Scrape tous les documents
        documents = scraper.scrape_all_documents()
        
        if documents:
            # Compare avec les données précédentes
            print("\nComparaison avec les données précédentes...")
            changes = scraper.compare_versions(previous_data)
            
            # Sauvegarde les données avec backup
            scraper.save_to_csv("pci_documents.csv", backup_previous=True)
            
            # Génère un rapport de changements si des changements sont détectés
            total_changes = (len(changes['new_documents']) + 
                           len(changes['updated_versions']) + 
                           len(changes['removed_documents']))
            
            if total_changes > 0:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                scraper.save_changes_report(changes, timestamp)
                
                print(f"\nCHANGEMENTS DÉTECTÉS!")
                print(f"Nouveaux documents: {len(changes['new_documents'])}")
                print(f"Versions mises à jour: {len(changes['updated_versions'])}")
                print(f"Documents supprimés: {len(changes['removed_documents'])}")
                print(f"Rapport détaillé sauvegardé dans: changes_report_{timestamp}.txt")
            else:
                print(f"\nAucun changement détecté depuis la dernière exécution")
                print(f"📊 {len(changes['unchanged_documents'])} documents inchangés")
            
            print(f"\nScraping terminé avec succès!")
            print(f"{len(documents)} documents extraits au total")
            print(f"Données sauvegardées dans: pci_documents.csv")
            
        else:
            print("Aucun document n'a pu être extrait")
    
    except Exception as e:
        logger.error(f"Erreur dans le programme principal: {e}")
        print(f"Erreur: {e}")
    
    finally:
        if scraper:
            scraper.close()

def main_comparison_only():
    """Fonction pour comparer uniquement les données existantes (sans scraping)"""
    try:
        print("Mode comparaison uniquement - Chargement des données...")
        
        # Charge les deux derniers fichiers CSV pour comparaison
        csv_files = glob.glob("/Users/thomasmionnet/Desktop/pci-dss/scraping2/pci_documents*.csv")
        csv_files.sort(key=os.path.getmtime, reverse=True)
        
        if len(csv_files) < 2:
            print("Il faut au moins 2 fichiers CSV pour effectuer une comparaison")
            return
        
        current_file = csv_files[0]
        previous_file = csv_files[1]
        
        print(f"Fichier actuel: {os.path.basename(current_file)}")
        print(f"Fichier précédent: {os.path.basename(previous_file)}")
        
        current_data = pd.read_csv(current_file)
        previous_data = pd.read_csv(previous_file)
        
        # Simule un scraper pour utiliser la méthode de comparaison
        scraper = PCIDocumentScraper(headless=True)
        scraper.documents = current_data.to_dict('records')
        
        changes = scraper.compare_versions(previous_data)
        
        # Génère un rapport
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scraper.save_changes_report(changes, f"comparison_{timestamp}")
        
        total_changes = (len(changes['new_documents']) + 
                        len(changes['updated_versions']) + 
                        len(changes['removed_documents']))
        
        if total_changes > 0:
            print(f"\nCHANGEMENTS DÉTECTÉS!")
            print(f"Nouveaux documents: {len(changes['new_documents'])}")
            print(f"Versions mises à jour: {len(changes['updated_versions'])}")
            print(f"Documents supprimés: {len(changes['removed_documents'])}")
        else:
            print(f"\nAucun changement détecté")
        
        print(f"Rapport sauvegardé dans: changes_report_comparison_{timestamp}.txt")
        
    except Exception as e:
        logger.error(f"Erreur lors de la comparaison: {e}")
        print(f"Erreur: {e}")

if __name__ == "__main__":
    main()
