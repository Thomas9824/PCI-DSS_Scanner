#!/usr/bin/env python3
"""
PCI Auto Scraper - Orchestrateur principal du système de monitoring PCI DSS/SAQ
Combine la détection de changements et le téléchargement automatique des documents PCI DSS/SAQ
Architecture : Change Detection -> Selective Download -> Extraction -> Email Notification
"""

import os  # Manipulation des chemins et système de fichiers
import sys  # Configuration des chemins Python et gestion des erreurs système
import time  # Mesure du temps d'exécution et timestamps
import logging  # Système de logging structuré
import base64  # Encodage des pièces jointes email
from datetime import datetime  # Génération de timestamps et horodatage
from typing import Dict, List, Optional  # Annotations de types pour la documentation du code
import shutil  # Opérations de copie et archivage des fichiers
import pandas as pd  # Manipulation des données CSV et DataFrames
import resend  # Service d'envoi d'emails pour les notifications

# Configuration des chemins des modules : architecture modulaire avec 3 composants principaux
script_dir = os.path.dirname(os.path.abspath(__file__))
pci_change_scraper_path = os.path.join(script_dir, 'pci_change_scraper')  # Module de détection des changements
pci_pdf_scraper_path = os.path.join(script_dir, 'pci_pdf_scraper')        # Module de téléchargement PDF
pci_pdf_extractor_path = os.path.join(script_dir, 'pci_pdf_extractor')    # Module d'extraction multilingue

# Injection des modules dans le PATH pour import dynamique
sys.path.insert(0, pci_change_scraper_path)
sys.path.insert(0, pci_pdf_scraper_path)
sys.path.insert(0, pci_pdf_extractor_path)

# Import des modules spécialisés avec gestion d'erreur
try:
    from pci_scraper import PCIDocumentScraper                                  # Scraper de détection de changements
    from pci_pdf_scraper import PCIScraperEnhanced                             # Téléchargeur PDF amélioré avec anti-403
    from testv5 import PCIRequirementsExtractor as PCIRequirementsExtractorFR  # Extracteur français
    from testv5_EN import PCIRequirementsExtractor as PCIRequirementsExtractorEN  # Extracteur anglais
    from testv5_ES import PCIRequirementsExtractor as PCIRequirementsExtractorES  # Extracteur espagnol
    from testv5_DE import PCIRequirementsExtractor as PCIRequirementsExtractorDE  # Extracteur allemand
    from testv5_PT import PCIRequirementsExtractor as PCIRequirementsExtractorPT  # Extracteur portugais
except ImportError as e:
    print(f"❌ Erreur d'import des modules: {e}")
    print("💡 Installez les dépendances avec: pip install -r requirements.txt")
    sys.exit(1)

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration du service d'email Resend pour les notifications automatiques
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement (.env)
load_dotenv()

# Initialisation de l'API Resend avec validation obligatoire
resend.api_key = os.getenv('RESEND_API_KEY')
if not resend.api_key:
    logger.error("RESEND_API_KEY non trouvée dans les variables d'environnement. Créez un fichier .env avec votre clé API.")
    sys.exit(1)

class PCIAutoScraper:
    """
    Orchestrateur principal combinant détection de changements, téléchargement sélectif et extraction multilingue
    Pipeline: Change Detection -> Selective Download -> Multi-language Extraction -> Email Report
    """

    def __init__(self, headless: bool = True, download_dir: str = "downloads"):
        """
        Initialise l'orchestrateur avec configuration par défaut pour automation

        Args:
            headless: Mode headless pour les navigateurs (True pour automation)
            download_dir: Répertoire de téléchargement des PDFs avec sessions horodatées
        """
        self.headless = headless
        self.download_dir = download_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Session unique horodatée

        # Instances des modules spécialisés (lazy loading)
        self.change_detector = None
        self.pdf_downloader = None

        # Métriques de performance et traçabilité
        self.stats = {
            'documents_checked': 0,      # Nombre total de documents vérifiés
            'changes_detected': 0,       # Nombre de changements détectés
            'downloads_attempted': 0,    # Tentatives de téléchargement
            'downloads_successful': 0,   # Téléchargements réussis
            'new_documents': 0,          # Nouveaux documents
            'updated_versions': 0,       # Versions mises à jour
            'removed_documents': 0,      # Documents supprimés
            'extracted_files': 0         # Fichiers CSV générés
        }

        # Stockage des fichiers CSV générés pour transmission par email
        self.extracted_csv_files = []
        
    def setup_scrapers(self):
        """Initialise et configure les modules avec patching pour intégration centralisée"""
        try:
            logger.info("Configuration des scrapers...")

            # Initialisation du détecteur de changements (Selenium-based)
            self.change_detector = PCIDocumentScraper(headless=self.headless)

            # Configuration des chemins centralisés (override des chemins hardcodés des modules)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_dir = script_dir  # Répertoire centralisé pour les données
            
            # Patching dynamique : Override des méthodes utilisant des chemins hardcodés
            original_load = self.change_detector.load_previous_data
            original_save = self.change_detector.save_to_csv
            original_save_report = self.change_detector.save_changes_report

            def patched_load_previous_data(filename="pci_documents.csv"):
                """Chargement des données de référence avec gestion centralisée des chemins"""
                try:
                    csv_path = os.path.join(self.data_dir, filename)
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
            
            def patched_save_to_csv(filename="pci_documents.csv", backup_previous=True):
                """Sauvegarde avec backup automatique et horodatage"""
                try:
                    if not self.change_detector.documents:
                        logger.warning("Aucun document à sauvegarder")
                        return

                    csv_path = os.path.join(self.data_dir, filename)

                    # Système de backup automatique avec timestamp
                    if backup_previous and os.path.exists(csv_path):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        backup_filename = f"pci_documents_backup_{timestamp}.csv"
                        backup_path = os.path.join(self.data_dir, backup_filename)

                        shutil.copy2(csv_path, backup_path)
                        logger.info(f"Ancienne version sauvegardée dans: {backup_filename}")

                    # Génération DataFrame avec métadonnées
                    df = pd.DataFrame(self.change_detector.documents)
                    df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Timestamp de mise à jour

                    # Persistance CSV avec encodage UTF-8
                    df.to_csv(csv_path, index=False, encoding='utf-8')

                    logger.info(f"Documents sauvegardés dans: {csv_path}")
                    logger.info(f"Nombre de documents sauvegardés: {len(self.change_detector.documents)}")

                except Exception as e:
                    logger.error(f"Erreur lors de la sauvegarde: {e}")
            
            def patched_save_changes_report(changes, timestamp=None):
                """Désactivation des rapports fichiers - reporting via email uniquement"""
                pass

            # Application des patches dynamiques aux modules
            self.change_detector.load_previous_data = patched_load_previous_data
            self.change_detector.save_to_csv = patched_save_to_csv
            self.change_detector.save_changes_report = patched_save_changes_report

            # Initialisation du téléchargeur PDF avec anti-403 et répertoire absolu
            full_download_path = os.path.abspath(self.download_dir)
            self.pdf_downloader = PCIScraperEnhanced(download_dir=full_download_path)
            
            return True
            
        except Exception as e:
            logger.error(f" Erreur lors de la configuration des scrapers: {e}")
            return False
    
    def detect_changes(self) -> Optional[Dict[str, List]]:
        """
        Pipeline de détection des changements : scraping -> comparaison -> persistance

        Returns:
            Dict contenant 'new_documents', 'updated_versions', 'removed_documents' ou None si erreur
        """
        try:
            logger.info("🔍 Démarrage de la détection de changements...")

            # Phase 1: Chargement des données de référence
            previous_data = self.change_detector.load_previous_data("pci_documents.csv")

            # Phase 2: Configuration driver Selenium
            self.change_detector.setup_driver()

            # Phase 3: Scraping complet des documents actuels (PCI DSS + SAQ)
            current_documents = self.change_detector.scrape_all_documents()
            self.stats['documents_checked'] = len(current_documents)
            
            if not current_documents:
                logger.error("Aucun document trouvé lors du scraping")
                return None

            # Phase 4: Analyse comparative avec les données de référence
            changes = self.change_detector.compare_versions(previous_data)

            # Phase 5: Mise à jour des métriques de changement
            self.stats['new_documents'] = len(changes['new_documents'])
            self.stats['updated_versions'] = len(changes['updated_versions'])
            self.stats['removed_documents'] = len(changes['removed_documents'])
            self.stats['changes_detected'] = (
                self.stats['new_documents'] +
                self.stats['updated_versions'] +
                self.stats['removed_documents']
            )

            # Phase 6: Persistance des nouvelles données avec backup automatique
            self.change_detector.save_to_csv("pci_documents.csv", backup_previous=True)
            
            # Phase 7: Logging des changements détectés (pas de fichier rapport)
            if self.stats['changes_detected'] > 0:
                logger.info(f"{self.stats['changes_detected']} changements détectés !")
                # Log détaillé des nouveaux documents
                for doc in changes['new_documents']:
                    logger.info(f"Nouveau: {doc['name']} ({doc['category']})")
                # Log détaillé des mises à jour de version
                for change in changes['updated_versions']:
                    logger.info(f"Mis à jour: {change['name']} ({change['category']}) - {change['old_version']} → {change['new_version']}")
            else:
                logger.info("Aucun changement détecté")
            
            return changes
            
        except Exception as e:
            logger.error(f"Erreur lors de la détection de changements: {e}")
            return None
        finally:
            if self.change_detector and self.change_detector.driver:
                self.change_detector.close()
    
    def should_download(self, changes: Dict[str, List]) -> bool:
        """
        Logique de décision pour le téléchargement sélectif

        Args:
            changes: Dictionnaire des changements détectés

        Returns:
            True si téléchargement requis (nouveaux documents ou versions mises à jour)
        """
        # Calcul des changements nécessitant un téléchargement (exclut les suppressions)
        total_changes = (
            len(changes['new_documents']) +
            len(changes['updated_versions'])
        )

        if total_changes > 0:
            logger.info(f"{total_changes} changements nécessitent un téléchargement")
            return True
        else:
            logger.info("Aucun téléchargement nécessaire")
            return False
    
    def download_changed_documents(self, changes: Dict[str, List]) -> bool:
        """
        Pipeline de téléchargement sélectif : collecte -> téléchargement -> extraction -> archivage

        Args:
            changes: Dictionnaire des changements détectés

        Returns:
            True si succès complet du pipeline, False en cas d'erreur
        """
        try:
            logger.info("📥 Démarrage du téléchargement sélectif des documents modifiés...")

            # Phase 1: Collecte des documents cibles
            documents_to_download = []

            # Ajout des nouveaux documents
            for doc in changes['new_documents']:
                documents_to_download.append(doc)
                logger.info(f"À télécharger (nouveau): {doc['name']} ({doc['category']})")

            # Ajout des documents avec versions mises à jour
            for change in changes['updated_versions']:
                doc_info = {
                    'name': change['name'],
                    'category': change['category'],
                    'version': change['new_version']
                }
                documents_to_download.append(doc_info)
                logger.info(f"À télécharger (mis à jour): {change['name']} ({change['category']}) - {change['old_version']} → {change['new_version']}")
            
            if not documents_to_download:
                logger.info("Aucun document à télécharger")
                return True
            
            logger.info(f"{len(documents_to_download)} documents à télécharger")
            
            # Crée un dossier spécifique pour cette session de téléchargement
            session_dir = os.path.join(self.download_dir, f"session_{self.timestamp}")
            os.makedirs(session_dir, exist_ok=True)
            
            # Télécharge sélectivement les documents
            success = self.download_specific_documents(documents_to_download, session_dir)
            
            if success:
                # Copie les nouveaux fichiers dans le répertoire principal
                main_download_dir = os.path.join(self.download_dir, "latest")
                os.makedirs(main_download_dir, exist_ok=True)
                
                downloaded_files = [f for f in os.listdir(session_dir) if f.endswith('.pdf')]
                self.stats['downloads_successful'] = len(downloaded_files)
                
                for file in downloaded_files:
                    src = os.path.join(session_dir, file)
                    dst = os.path.join(main_download_dir, file)
                    shutil.copy2(src, dst)
                    logger.info(f"📄 Copié: {file}")
                
                # Extraction automatique des PDFs téléchargés
                logger.info("🔍 Démarrage de l'extraction des exigences PCI DSS...")
                self.extract_downloaded_pdfs(downloaded_files, session_dir)
                
                logger.info(f"{len(downloaded_files)} documents téléchargés avec succès")
                return True
            else:
                logger.error("Échec du téléchargement sélectif")
                return False
                
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement sélectif: {e}")
            return False
    
    def download_specific_documents(self, documents_to_download: List[Dict], download_dir: str) -> bool:
        """
        Téléchargement sélectif avec filtrage précis et fallback automatique

        Args:
            documents_to_download: Liste des documents cibles avec métadonnées
            download_dir: Répertoire de destination

        Returns:
            True si téléchargement réussi avec au moins un fichier
        """
        try:
            logger.info(f"🔧 Configuration du téléchargeur pour {len(documents_to_download)} documents spécifiques")

            # Initialisation du téléchargeur avec anti-403 et stealth mode
            selective_downloader = PCIScraperEnhanced(download_dir=download_dir)

            # Monkey patching du téléchargeur pour filtrage sélectif
            original_get_all_pdf_links = selective_downloader.get_all_pdf_links

            def selective_get_pdf_links():
                """Filtrage précis basé sur les changements détectés avec matching multi-critères"""
                logger.info("Recherche des liens PDF pour les documents spécifiques...")

                # Récupération exhaustive des liens PDF avec métadonnées
                all_links = original_get_all_pdf_links()

                # Algorithme de filtrage précis avec matching multi-critères
                filtered_links = []

                for link_info in all_links:
                    # Matching précis nom/version/catégorie pour chaque document cible
                    for doc in documents_to_download:
                        if self.matches_document_precise(link_info, doc):
                            filtered_links.append(link_info)
                            logger.info(f"Lien trouvé pour: {doc['name']} (v{doc.get('version', 'N/A')}) -> {link_info['url']}")
                            break

                logger.info(f"📊 {len(filtered_links)} liens PDF filtrés sur {len(all_links)} disponibles")
                return filtered_links
            
            # Application du patch de filtrage sélectif
            selective_downloader.get_all_pdf_links = selective_get_pdf_links

            # Exécution du téléchargement sélectif avec anti-403
            selective_downloader.run()

            # Validation des résultats de téléchargement
            downloaded_files = [f for f in os.listdir(download_dir) if f.endswith('.pdf')] if os.path.exists(download_dir) else []

            if downloaded_files:
                logger.info(f"Téléchargement sélectif réussi: {len(downloaded_files)} fichiers")
                return True
            else:
                logger.warning("Aucun fichier téléchargé lors du téléchargement sélectif")
                # Stratégie de fallback : téléchargement des documents critiques
                return self.fallback_download(documents_to_download, download_dir)
                
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement spécifique: {e}")
            return False
    
    def matches_document_precise(self, link_info: Dict, target_doc: Dict) -> bool:
        """
        Algorithme de matching multi-critères pour correspondance exacte document/lien

        Args:
            link_info: Métadonnées du lien (document_name, version, category)
            target_doc: Document cible avec critères de recherche

        Returns:
            True si matching exact sur nom + catégorie + version (si disponible)
        """
        try:
            # Extraction et normalisation des métadonnées du lien
            link_doc_name = link_info.get('document_name', '').lower().strip()
            link_version = link_info.get('version', '').lower().strip()
            link_category = link_info.get('category', '').lower().strip()

            # Extraction et normalisation des critères du document cible
            target_name = target_doc.get('name', '').lower().strip()
            target_version = target_doc.get('version', '').lower().strip()
            target_category = target_doc.get('category', '').lower().strip()

            # Algorithme de matching : nom exact + catégorie flexible
            name_match = link_doc_name == target_name
            category_match = any(cat in link_category for cat in [target_category, target_category.replace(' ', '')])
            
            # Matching conditionnel des versions (si disponibles)
            version_match = True
            if target_version and target_version != 'n/a' and link_version and link_version != 'n/a':
                # Normalisation et comparaison des versions
                target_version_clean = self.normalize_version(target_version)
                link_version_clean = self.normalize_version(link_version)
                version_match = target_version_clean == link_version_clean

            # Calcul du résultat final du matching (AND logique)
            match_result = name_match and category_match and version_match
            
            if match_result:
                logger.info(f"Match précis trouvé: '{link_doc_name}' v{link_version} == '{target_name}' v{target_version}")
            else:
                logger.debug(f"Pas de match: '{link_doc_name}' v{link_version} != '{target_name}' v{target_version}")
                logger.debug(f"Name match: {name_match}, Category match: {category_match}, Version match: {version_match}")
            
            return match_result
            
        except Exception as e:
            logger.warning(f"Erreur lors du matching précis: {e}")
            return False
    
    def normalize_version(self, version: str) -> str:
        """
        Normalise une version pour la comparaison
        
        Args:
            version: Version à normaliser
            
        Returns:
            Version normalisée
        """
        if not version:
            return ""
        
        # Supprime les espaces et convertit en minuscules
        normalized = version.lower().strip()
        
        # Supprime les caractères non essentiels
        import re
        normalized = re.sub(r'[^\w\.]', '', normalized)
        
        return normalized

    def extract_downloaded_pdfs(self, downloaded_files: List[str], session_dir: str):
        """
        Pipeline d'extraction multilingue : détection langue -> extraction -> sauvegarde CSV

        Args:
            downloaded_files: Liste des PDFs téléchargés
            session_dir: Répertoire de travail de la session
        """
        try:
            for pdf_file in downloaded_files:
                pdf_path = os.path.join(session_dir, pdf_file)

                # Détection automatique de la langue basée sur les indicateurs du nom de fichier
                pdf_name_without_ext = os.path.splitext(pdf_file)[0]
                language = self.detect_document_language(pdf_file)

                logger.info(f"🔍 Analyse de langue pour {pdf_file}: {language}")

                # Sélection de l'extracteur spécialisé selon la langue détectée
                if language == 'FR':
                    logger.info(f"📋 Extraction FR: {pdf_file}")
                    extractor = PCIRequirementsExtractorFR(pdf_path)
                elif language == 'ES':
                    logger.info(f"📋 Extraction ES: {pdf_file}")
                    extractor = PCIRequirementsExtractorES(pdf_path)
                elif language == 'DE':
                    logger.info(f"📋 Extraction DE: {pdf_file}")
                    extractor = PCIRequirementsExtractorDE(pdf_path)
                elif language == 'PT':
                    logger.info(f"📋 Extraction PT: {pdf_file}")
                    extractor = PCIRequirementsExtractorPT(pdf_path)
                else:  # EN par défaut (fallback)
                    logger.info(f"📋 Extraction EN: {pdf_file}")
                    extractor = PCIRequirementsExtractorEN(pdf_path)
                
                output_file = os.path.join(session_dir, f"{pdf_name_without_ext}.csv")

                # Extraction des exigences PCI DSS avec l'extracteur spécialisé
                requirements = extractor.extract_all_requirements()

                if requirements:
                    # Sauvegarde des exigences en format CSV
                    extractor.save_to_csv(output_file)

                    # Enregistrement du fichier pour inclusion dans l'email de rapport
                    self.extracted_csv_files.append(output_file)
                    self.stats['extracted_files'] += 1

                    language = self.detect_document_language(pdf_file)
                    logger.info(f"✅ Extraction {language} réussie: {len(requirements)} exigences → {os.path.basename(output_file)}")
                else:
                    logger.warning(f"⚠️ Aucune exigence extraite de {pdf_file}")
                    
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des PDFs: {e}")
    
    def detect_document_language(self, filename: str) -> str:
        """
        Détecteur automatique de langue basé sur les patterns du nom de fichier

        Args:
            filename: Nom du fichier PDF ou CSV à analyser

        Returns:
            Code de langue ISO ('EN', 'FR', 'ES', 'DE', 'PT') avec fallback EN
        """
        filename_lower = filename.lower()

        # Mapping des indicateurs linguistiques par ordre de priorité
        language_indicators = {
            'EN': ['_en.pdf', '_en.csv', '-en.pdf', '-en.csv', '_en_', '-en_', 'english'],
            'FR': ['_fr.pdf', '_fr.csv', '-fr.pdf', '-fr.csv', '_fr_', '-fr_', 'french', 'francais', 'merchant-fr', '-merchant-fr'],
            'ES': ['_es.pdf', '_es.csv', '-es.pdf', '-es.csv', '_es_', '-es_', 'spanish', 'espanol', 'merchant-es', '-merchant-es'],
            'DE': ['_de.pdf', '_de.csv', '-de.pdf', '-de.csv', '_de_', '-de_', 'german', 'deutsch', 'merchant-de', '-merchant-de'],
            'PT': ['_pt.pdf', '_pt.csv', '-pt.pdf', '-pt.csv', '_pt_', '-pt_', 'portuguese', 'portugues', 'merchant-pt', '-merchant-pt']
        }

        # Algorithme de détection par pattern matching
        for lang_code, indicators in language_indicators.items():
            if any(indicator in filename_lower for indicator in indicators):
                return lang_code

        # Fallback : anglais par défaut si aucun indicateur détecté
        return 'EN'

    def matches_document(self, url: str, doc_name: str, doc_category: str) -> bool:
        """
        Vérifie si une URL correspond EXACTEMENT à un document recherché
        
        Args:
            url: URL du document
            doc_name: Nom du document recherché
            doc_category: Catégorie du document
            
        Returns:
            True si l'URL correspond exactement au document
        """
        url_lower = url.lower()
        doc_name_lower = doc_name.lower()
        
        # Mapping précis des noms de documents vers leurs URLs spécifiques
        exact_mappings = {
            # Documents PCI DSS principaux
            'pci dss': ['pci-dss-v4_0_1.pdf', 'pci-dss-v4-0-1.pdf'],
            'pci dss summary of changes': ['pci-dss-v4-0-to-v4-0-1-summary-of-changes', 'summary-of-changes'],
            'prioritized approach for pci dss': ['prioritized-approach-for-pci-dss'],
            'prioritized approach tool': ['prioritized-approach'],
            'pci dss quick reference guide': ['pci-dss-v4_x-qrg', 'quick-reference'],
            'pci dss v4.0 at a glance': ['pci-dss-v4-0-at-a-glance'],
            'asv resource guide': ['pci%20ssc%20asv%20resource%20guide', 'asv-resource'],
            'guidance for pci dss requirements': ['guidance-for-pci-dss-requirements'],
            'tra guidance': ['pci_dss_v4.x_tra_guidance', 'tra-guidance'],
            'vulnerability management': ['vulnerability%20management%20infographic'],
            'roc template': ['pci-dss-v4-0-1-roc-template'],
            'roc template summary of changes': ['roc-template-summary-of-changes'],
            
            # Documents SAQ spécifiques
            'saq instructions and guidelines': ['saq-instructions-guidelines-pci-dss'],
            'saq a': ['pci-dss-v4_0_1-saq-a-r1.pdf', 'pci-dss-v4-0-1-saq-a.pdf'],
            'saq a-ep': ['pci-dss-v4-0-1-saq-a-ep.pdf'],
            'saq b': ['pci-dss-v4-0-1-saq-b.pdf'],
            'saq b-ip': ['pci-dss-v4-0-1-saq-b-ip.pdf'],
            'saq c': ['pci-dss-v4-0-1-saq-c.pdf'],
            'saq c-vt': ['pci-dss-v4-0-1-saq-c-vt.pdf'],
            'saq d merchant': ['pci-dss-v4-0-1-saq-d-merchant.pdf'],
            'saq d service provider': ['pci-dss-v4-0-1-saq-d-service-provider'],
            'saq p2pe': ['pci-dss-v4-0-1-saq-p2pe.pdf'],
            'saq spoc': ['pci-dss-v4-0-1-saq-spoc.pdf'],
            
            # AOC documents
            'aoc saq a': ['pci-dss-v4-0-1-aoc-for-saq-a'],
            'aoc saq a-ep': ['pci-dss-v4-0-1-aoc-for-saq-a-ep'],
            'aoc saq b': ['pci-dss-v4-0-1-aoc-for-saq-b.pdf'],
            'aoc saq b-ip': ['pci-dss-v4-0-1-aoc-for-saq-b-ip'],
            'aoc saq c': ['pci-dss-v4-0-1-aoc-for-saq-c.pdf'],
            'aoc saq c-vt': ['pci-dss-v4-0-1-aoc-for-saq-c-vt'],
            'aoc saq d - merchants': ['pci-dss-v4-0-1-aoc-for-saq-d-merchant'],
            'aoc saq d - service providers': ['pci-dss-v4-0-1-aoc-for-saq-d-service-provider'],
            'aoc saq p2pe': ['pci-dss-v4-0-1-aoc-for-saq-p2pe'],
            'aoc saq spoc': ['pci-dss-v4-0-1-aoc-for-saq-spoc'],
        }
        
        # Recherche exacte d'abord
        for doc_pattern, url_patterns in exact_mappings.items():
            if doc_pattern in doc_name_lower:
                for pattern in url_patterns:
                    if pattern.lower() in url_lower:
                        return True
        
        # Si pas de match exact, utilise une logique plus stricte
        # Seulement pour les cas où le nom du document correspond très précisément
        
        # Pour SAQ Instructions and Guidelines - cas spécial car il guide tous les SAQ
        if 'saq instructions and guidelines' in doc_name_lower:
            return 'saq-instructions-guidelines' in url_lower
        
        # Pour les documents PCI DSS principaux
        if doc_name_lower == 'pci dss':
            return 'pci-dss-v4_0_1.pdf' in url_lower or 'pci-dss-v4-0-1.pdf' in url_lower
        
        # Pour les SAQ spécifiques - match très précis
        saq_types = ['saq a', 'saq b', 'saq c', 'saq d', 'saq p2pe', 'saq spoc']
        for saq_type in saq_types:
            if doc_name_lower.startswith(saq_type) and saq_type.replace(' ', '-') in url_lower:
                # Vérification supplémentaire pour éviter les faux positifs
                if saq_type == 'saq a' and ('saq-a-ep' in url_lower or 'saq-a-r1' in url_lower):
                    return 'ep' in doc_name_lower or 'r1' in url_lower
                return True
        
        return False
    
    def fallback_download(self, documents_to_download: List[Dict], download_dir: str) -> bool:
        """
        Téléchargement de fallback si le téléchargement sélectif échoue
        
        Args:
            documents_to_download: Documents à télécharger
            download_dir: Répertoire de téléchargement
            
        Returns:
            True si au moins quelques documents sont téléchargés
        """
        
        try:
            # Essaie de télécharger les documents les plus critiques
            critical_docs = [doc for doc in documents_to_download if 
                           'pci dss' in doc['name'].lower() or 'saq a' in doc['name'].lower()]
            
            if critical_docs:
                
                # Lance un téléchargement complet mais limité
                fallback_downloader = PCIScraperEnhanced(download_dir=download_dir)
                fallback_downloader.run()
                
                downloaded_files = [f for f in os.listdir(download_dir) if f.endswith('.pdf')] if os.path.exists(download_dir) else []
                
                if downloaded_files:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement de fallback: {e}")
            return False
    
    def log_session_summary(self, changes: Dict[str, List] = None):
        """
        Affiche un résumé de session dans les logs (sans créer de fichier)
        
        Args:
            changes: Dictionnaire des changements détectés (optionnel)
        """
        logger.info("RÉSUMÉ DE SESSION:")
        logger.info(f"   Documents vérifiés: {self.stats['documents_checked']}")
        logger.info(f"   Changements détectés: {self.stats['changes_detected']}")
        logger.info(f"   Téléchargements réussis: {self.stats['downloads_successful']}")
        logger.info(f"   Fichiers extraits (CSV): {self.stats['extracted_files']}")
        
        if changes and self.stats['changes_detected'] > 0:
            logger.info("DÉTAIL DES CHANGEMENTS:")
            logger.info(f"   • Nouveaux documents: {len(changes['new_documents'])}")
            logger.info(f"   • Versions mises à jour: {len(changes['updated_versions'])}")
            logger.info(f"   • Documents supprimés: {len(changes['removed_documents'])}")
        
        if self.extracted_csv_files:
            logger.info("FICHIERS CSV GÉNÉRÉS:")
            for csv_file in self.extracted_csv_files:
                logger.info(f"   📄 {os.path.basename(csv_file)}")
        
        status = "SUCCÈS" if self.stats['changes_detected'] == 0 or self.stats['downloads_successful'] > 0 else "⚠️ PARTIEL"
        logger.info(f"STATUT FINAL: {status}")
    
    def send_email_summary(self, changes: Dict[str, List] = None, execution_time: float = 0):
        """
        Générateur et envoyeur de rapport HTML par email avec pièces jointes CSV

        Args:
            changes: Dictionnaire des changements détectés (optionnel)
            execution_time: Temps d'exécution total en secondes
        """
        try:
            logger.info("📧 Envoi du récapitulatif par email...")

            # Calcul du statut de la session basé sur les métriques
            if self.stats['changes_detected'] == 0:
                status = "Aucun Changement"
                status_class = "status-success"
            elif self.stats['downloads_successful'] > 0:
                status = "Succès"
                status_class = "status-success"
            else:
                status = "Problème"
                status_class = "status-warning"

            # Génération du template HTML responsive avec CSS intégré
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * {{ font-family: "Geist", sans-serif; font-weight: 100; }}
        html, body {{ margin: 0; padding: 0; height: 100%; background-color: #f5f5f5; }}
        body {{ color: white; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .main-title {{ font-size: 96px; line-height: 0.8; font-weight: 100; color: black; text-align: left; margin-bottom: 30px; }}
        .card {{ background: black; padding: 20px; border-radius: 12px; margin: 10px 0; position: relative; }}
        .title {{ font-size: 28px; margin-bottom: 8px; font-weight: 100; }}
        .subtitle {{ font-size: 14px; color: #ccc; margin-bottom: 20px; font-weight: 100; }}
        .separator {{ height: 1px; background: #333; margin: 20px 0; }}
        .section-title {{ font-size: 18px; margin-bottom: 15px; font-weight: 100; }}
        .stats-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .stat-item {{ text-align: center; }}
        .stat-value {{ font-size: 20px; font-weight: 100; }}
        .stat-label {{ font-size: 14px; color: #ccc; font-weight: 100; }}
        .changes-section {{ margin-top: 20px; }}
        .doc-list {{ margin: 10px 0; }}
        .doc-item {{ padding: 8px 0; border-bottom: 1px solid #333; font-weight: 100; }}
        .doc-item:last-child {{ border-bottom: none; }}
        .status-indicator {{ position: absolute; top: 20px; right: 20px; display: flex; align-items: center; gap: 8px; }}
        .status-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
        .status-text {{ font-size: 12px; color: #ccc; font-weight: 100; }}
        .status-success {{ background-color: #28a745; }}
        .status-warning {{ background-color: #ffc107; }}
        .status-error {{ background-color: #dc3545; }}
        strong {{ font-weight: 100; }}
        .csv-link {{ color: white; text-decoration: none; font-weight: 100; transition: all 0.2s ease; }}
        .csv-link:hover {{ text-decoration: underline; color: #f0f0f0; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="main-title">PCI-DSS<br>Scanner</div>
        <div class="card">
            <div class="status-indicator">
                <div class="status-dot {status_class}"></div>
                <div class="status-text">Status</div>
            </div>
            
            <div class="title">Session Statistics</div>
            <div class="subtitle">Scan on {datetime.now().strftime('%d/%m/%Y at %I:%M:%S %p')}</div>
            
            <div class="separator"></div>
            
            <div class="section-title">Changes Details</div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-value">{len(changes['new_documents']) if changes else 0}</div>
                    <div class="stat-label">New documents</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{len(changes['updated_versions']) if changes else 0}</div>
                    <div class="stat-label">Updated versions</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{self.stats['downloads_successful']}</div>
                    <div class="stat-label">Successful downloads</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{self.stats['extracted_files']}</div>
                    <div class="stat-label">Extracted CSV files</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{execution_time:.2f} seconds</div>
                    <div class="stat-label">Execution time</div>
                </div>
            </div>
            
            <div class="separator"></div>"""
            
            # Ajoute les détails des changements si il y en a
            if changes and self.stats['changes_detected'] > 0:
                html_content += """
            <div class="changes-section">"""
                
                # Liste les nouveaux documents
                if changes['new_documents']:
                    html_content += '<div style="margin-top: 20px;"><strong>New documents:</strong></div><div class="doc-list">'
                    for doc in changes['new_documents']:
                        html_content += f'<div class="doc-item">{doc["name"]} ({doc["category"]})</div>'
                    html_content += "</div>"
                
                # Liste les documents mis à jour
                if changes['updated_versions']:
                    html_content += '<div class="section-title" style="margin-top: 20px;"><strong>Updated documents:</strong></div><div class="doc-list">'
                    for change in changes['updated_versions']:
                        html_content += f'<div class="doc-item">{change["name"]} ({change["category"]}) - {change["old_version"]} → {change["new_version"]}</div>'
                    html_content += "</div>"
                
                html_content += "</div>"
            
            # Ajoute la section des fichiers CSV extraits
            if self.extracted_csv_files:
                html_content += """
            <div class="changes-section">
                <div class="section-title" style="margin-top: 20px;"><strong>CSV Files Generated:</strong></div>
                <div class="doc-list">"""
                
                for csv_file in self.extracted_csv_files:
                    filename = os.path.basename(csv_file)
                    
                    # Détermine la langue basée sur le nom de fichier pour l'affichage
                    language = self.detect_document_language(filename)
                    
                    # Mapping des langues vers drapeaux et labels
                    language_display = {
                        'EN': ("🇬🇧", "EN"),
                        'FR': ("🇫🇷", "FR"),
                        'ES': ("🇪🇸", "ES"),
                        'DE': ("🇩🇪", "DE"),
                        'PT': ("🇵🇹", "PT")
                    }
                    
                    flag_emoji, lang_label = language_display.get(language, ("🇬🇧", "EN"))
                    
                    try:
                        # Lire le contenu CSV pour créer un lien de téléchargement direct
                        with open(csv_file, 'r', encoding='utf-8') as f:
                            csv_content = f.read()
                        
                        # Créer un data URL pour téléchargement direct
                        import urllib.parse
                        csv_encoded = urllib.parse.quote(csv_content)
                        data_url = f"data:text/csv;charset=utf-8,{csv_encoded}"
                        
                        html_content += f'<div class="doc-item"><a href="{data_url}" download="{filename}" class="csv-link">{flag_emoji} {filename}</a> <span style="color: #ccc; font-size: 12px;">({lang_label} - click to download)</span></div>'
                    except Exception as e:
                        # Fallback si erreur de lecture
                        html_content += f'<div class="doc-item"><span class="csv-link">{flag_emoji} {filename}</span> <span style="color: #ccc; font-size: 12px;">({lang_label} - see attachments)</span></div>'
                
                html_content += """
                </div>
            </div>"""
            
            html_content += f"""
        </div>
    </div>
</body>
</html>"""
            
            # Détermine le sujet selon le statut
            if self.stats['changes_detected'] > 0:
                subject = f"PCI Scraper: {self.stats['changes_detected']} changement(s) détecté(s)"
            else:
                subject = "PCI Scraper: Aucun changement détecté"
            
            # Prépare l'email avec pièces jointes si des fichiers JSON sont disponibles
            email_data = {
                "from": "onboarding@resend.dev",
                "to": os.getenv('EMAIL_RECIPIENT', "mionnet.thom@gmail.com"), 
                "subject": subject,
                "html": html_content
            }
            
            # Ajoute les pièces jointes CSV
            if self.extracted_csv_files:
                attachments = []
                for csv_file_path in self.extracted_csv_files:
                    try:
                        with open(csv_file_path, 'r', encoding='utf-8') as f:
                            csv_content = f.read()
                        
                        # Encode en base64 pour l'envoi
                        csv_base64 = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
                        
                        filename = os.path.basename(csv_file_path)
                        attachment = {
                            "filename": filename,
                            "content": csv_base64
                        }
                        attachments.append(attachment)
                        logger.info(f"📎 Pièce jointe ajoutée: {filename}")
                        
                    except Exception as e:
                        logger.warning(f"Erreur lors de l'ajout de la pièce jointe {csv_file_path}: {e}")
                
                if attachments:
                    email_data["attachments"] = attachments
                    logger.info(f"📧 Email avec {len(attachments)} pièce(s) jointe(s)")
            
            # Envoie l'email
            response = resend.Emails.send(email_data)
            
            logger.info("Email récapitulatif envoyé avec succès")
            logger.info(f"ID de l'email: {response.get('id', 'N/A')}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi de l'email: {e}")
            return False

    def run(self) -> bool:
        """
        Méthode principale : orchestration complète du pipeline PCI DSS monitoring
        Workflow: Setup -> Change Detection -> Download Decision -> Selective Download -> Extraction -> Reporting

        Returns:
            True si pipeline exécuté avec succès, False en cas d'erreur critique
        """
        start_time = time.time()
        success = False
        changes = None
        
        try:
            logger.info("Démarrage du PCI Auto Scraper")
            logger.info(f"Session: {self.timestamp}")
            
            # Crée le répertoire de téléchargement
            os.makedirs(self.download_dir, exist_ok=True)
            
            # Configure les scrapers
            if not self.setup_scrapers():
                return False
            
            # Étape 1: Détection des changements
            logger.info("\n" + "="*50)
            logger.info("ÉTAPE 1: DÉTECTION DES CHANGEMENTS")
            logger.info("="*50)
            
            changes = self.detect_changes()
            if changes is None:
                logger.error("Échec de la détection de changements")
                return False
            
            # Étape 2: Décision de téléchargement
            logger.info("\n" + "="*50)
            logger.info("ÉTAPE 2: ANALYSE DES CHANGEMENTS")
            logger.info("="*50)
            
            should_download = self.should_download(changes)
            
            # Étape 3: Téléchargement si nécessaire
            if should_download:
                logger.info("\n" + "="*50)
                logger.info("ÉTAPE 3: TÉLÉCHARGEMENT DES DOCUMENTS")
                logger.info("="*50)
                
                download_success = self.download_changed_documents(changes)
                if download_success:
                    logger.info("Téléchargement terminé avec succès")
                    success = True
                else:
                    logger.warning("Téléchargement partiellement réussi ou échoué")
            else:
                logger.info("Aucun téléchargement nécessaire - tous les documents sont à jour")
                success = True
            
            return success
            
        except Exception as e:
            logger.error(f"Erreur dans le processus principal: {e}")
            return False
        
        finally:
            # Affiche le résumé final
            execution_time = time.time() - start_time
            logger.info(f"\nTemps d'exécution: {execution_time:.2f} secondes")
            
            self.log_session_summary(changes)
            
            # Envoi du récapitulatif par email
            self.send_email_summary(changes, execution_time)
            
            # Nettoyage
            if self.change_detector and self.change_detector.driver:
                self.change_detector.close()
            
            # Résumé final
            logger.info("\n" + "="*50)
            logger.info("RÉSUMÉ DE LA SESSION")
            logger.info("="*50)
            logger.info(f"Documents vérifiés: {self.stats['documents_checked']}")
            logger.info(f"Changements détectés: {self.stats['changes_detected']}")
            logger.info(f"Téléchargements réussis: {self.stats['downloads_successful']}")
            logger.info(f"Durée: {execution_time:.2f}s")
            logger.info(f"Statut: {'SUCCÈS' if success else 'ÉCHEC'}")

def main():
    """Point d'entrée principal - Initialisation et exécution du pipeline complet PCI DSS monitoring"""
    try:
        print("Démarrage du PCI Auto Scraper")
        print("Détection automatique des changements et téléchargement des documents PCI DSS/SAQ")
        print("=" * 80)

        # Initialisation de l'orchestrateur en mode automatisé (headless + téléchargements sélectifs)
        auto_scraper = PCIAutoScraper(
            headless=True,        # Mode headless pour automation et déploiement serveur
            download_dir='downloads'  # Répertoire de stockage avec sessions horodatées
        )
        
        success = auto_scraper.run()
        
        if success:
            print("\nPCI Auto Scraper terminé avec succès !")
            print("Vérifiez le dossier 'downloads' pour les nouveaux fichiers")
            sys.exit(0)
        else:
            print("\nPCI Auto Scraper terminé avec des erreurs")
            print("Consultez les logs ci-dessus pour plus de détails")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        print(f"\nErreur fatale: {e}")
        print("Vérifiez que les dépendances sont installées: pip install -r requirements.txt")
        sys.exit(1)

if __name__ == "__main__":
    main()
