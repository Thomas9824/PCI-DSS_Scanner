#!/usr/bin/env python3
"""
PCI Security Standards Document Scraper - Enhanced Version

Module spécialisé de téléchargement sélectif avec techniques anti-détection avancées.
Conçu pour contourner les protections 403 et télécharger uniquement les documents
SAQ (Self-Assessment Questionnaire) et PCI DSS avec support multi-langue.

Architecture:
- Selenium WebDriver avec selenium-stealth pour contournement anti-bot
- Rotation dynamique d'user-agents et headers HTTP réalistes
- Support des dropdowns de langue pour téléchargement multi-variantes
- Pipeline de retry avec backoff exponentiel pour robustesse
"""

import os  # Manipulation système de fichiers et chemins
import time  # Gestion des délais et temporisation anti-détection
import requests  # Client HTTP pour téléchargements avec cookies/sessions
from urllib.parse import urljoin, urlparse  # Parsing et construction d'URLs
from selenium import webdriver  # Automatisation navigateur pour contournement JavaScript
from selenium.webdriver.chrome.options import Options  # Configuration avancée Chrome
from selenium.webdriver.common.by import By  # Sélecteurs d'éléments DOM
from selenium.webdriver.support.ui import WebDriverWait  # Attentes conditionnelles
from selenium.webdriver.support import expected_conditions as EC  # Conditions d'attente prédéfinies
from selenium.webdriver.support.ui import Select  # Interaction avec dropdowns HTML
from selenium_stealth import stealth  # Bibliothèque anti-détection WebDriver
from bs4 import BeautifulSoup  # Parser HTML pour extraction de liens
import logging  # Système de logging structuré pour debugging
import random  # Génération de valeurs aléatoires pour randomisation

# Configuration du système de logging avec format timestamp
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)  # Logger spécifique au module

class PCIScraperEnhanced:
    """Scraper avancé pour documents PCI DSS avec techniques anti-détection

    Classe principale orchestrant le téléchargement sélectif de documents SAQ/PCI DSS
    avec contournement des protections web et support multi-langue automatique.
    """
    def __init__(self, download_dir="downloads"):
        """Initialisation avec configuration anti-détection et filtrage de catégories"""
        self.download_dir = download_dir  # Répertoire de destination des téléchargements
        self.base_url = "https://www.pcisecuritystandards.org/document_library/"  # URL cible officielle
        self.session = requests.Session()  # Session HTTP persistante pour cookies/auth
        self.driver = None  # Instance WebDriver (sera initialisée plus tard)
        
        # Définition des catégories cibles pour filtrage sélectif
        # Limite le téléchargement aux documents essentiels de conformité
        self.target_categories = [
            'SAQ',                        # Self-Assessment Questionnaires
            'PCI DSS',                    # Data Security Standard principal
            'Self-Assessment Questionnaire',  # Variant de nommage
            'Data Security Standard'      # Variant de nommage
        ]
        
        # Création automatique du répertoire de téléchargement
        os.makedirs(self.download_dir, exist_ok=True)  # exist_ok évite les erreurs si existe déjà
        
        # Pool d'user-agents réalistes pour rotation anti-détection
        # Contient des signatures de navigateurs légitimes récents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',  # Chrome Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',  # Chrome Windows legacy
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',  # Chrome macOS
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'  # Chrome Linux
        ]
        
        # Initialisation de la session avec headers rotatifs
        self.update_session_headers()  # Premier setup des headers HTTP

    def update_session_headers(self):
        """Mise à jour des headers HTTP avec rotation aléatoire d'user-agent

        Simule un navigateur réel avec headers complets pour éviter la détection
        """
        # Configuration complète des headers HTTP pour simulation navigateur réel
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),  # User-Agent rotatif aléatoire
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',  # Types de contenu acceptés
            'Accept-Language': 'en-US,en;q=0.9',  # Préférences linguistiques
            'Accept-Encoding': 'gzip, deflate, br',  # Méthodes de compression supportées
            'DNT': '1',  # Do Not Track pour confidentialité
            'Connection': 'keep-alive',  # Connexion persistante
            'Upgrade-Insecure-Requests': '1',  # Préférence HTTPS
            'Sec-Fetch-Dest': 'document',  # Contexte de requête sécurisée
            'Sec-Fetch-Mode': 'navigate',  # Mode de navigation
            'Sec-Fetch-Site': 'same-origin',  # Politique d'origine
            'Cache-Control': 'max-age=0'  # Contrôle du cache
        })

    def setup_driver(self):
        """Configuration avancée du driver Chrome avec techniques de contournement

        Applique selenium-stealth et masquage des signatures WebDriver pour éviter la détection
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Mode sans interface graphique
        chrome_options.add_argument("--no-sandbox")  # Contourne les restrictions de sandbox
        chrome_options.add_argument("--disable-dev-shm-usage")  # Évite les problèmes de mémoire partagée
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Cache les marqueurs d'automation
        chrome_options.add_argument("--disable-web-security")  # Désactive certaines vérifications de sécurité
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")  # Optimisation rendu
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])  # Supprime le flag automation
        chrome_options.add_experimental_option('useAutomationExtension', False)  # Désactive l'extension automation
        
        # Application d'un user-agent aléatoire au driver pour cohérence
        user_agent = random.choice(self.user_agents)
        chrome_options.add_argument(f"--user-agent={user_agent}")  # Synchronisation avec session HTTP
        
        self.driver = webdriver.Chrome(options=chrome_options)  # Initialisation du driver avec options
        
        # Application du module selenium-stealth pour masquage complet
        stealth(self.driver,
                languages=["en-US", "en"],  # Langues du navigateur simulé
                vendor="Google Inc.",  # Fabricant du navigateur
                platform="Win32",  # Plateforme système simulée
                webgl_vendor="Intel Inc.",  # Fabricant GPU simulé
                renderer="Intel Iris OpenGL Engine",  # Moteur de rendu simulé
                fix_hairline=True)  # Correction des artefacts de rendu
        
        # Suppression manuelle des signatures WebDriver via JavaScript
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")  # Masque navigator.webdriver
        self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")  # Simule des plugins réalistes
        self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")  # Force les langues

    def is_target_category(self, category_text):
        """Vérifie si une catégorie correspond aux critères de téléchargement

        Filtre sélectif pour ne traiter que les documents SAQ et PCI DSS
        """
        category_lower = category_text.lower()  # Normalisation en minuscules

        # Mots-clés de catégories cibles pour filtrage précis
        target_keywords = [
            'saq',                        # Self-Assessment Questionnaires
            'self-assessment questionnaire',  # Nom complet SAQ
            'pci dss',                    # Standard principal
            'data security standard',     # Nom alternatif PCI DSS
            'pci-dss'                    # Variant avec tiret
        ]

        # Vérification de correspondance avec les mots-clés cibles
        for keyword in target_keywords:
            if keyword in category_lower:
                return True  # Catégorie correspond aux critères

        return False  # Catégorie non ciblée

    def get_document_categories(self):
        """Récupère et filtre les catégories de documents disponibles

        Analyse le dropdown des catégories pour identifier uniquement SAQ et PCI DSS
        """
        try:
            # Attente de la présence du dropdown de catégories avec timeout
            category_dropdown = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "document_category"))  # Sélecteur par ID
            )
            
            select = Select(category_dropdown)  # Wrapper pour interaction avec dropdown
            categories = []  # Liste des catégories filtrées

            # Parcours de toutes les options disponibles
            for option in select.options:
                if option.get_attribute("value"):  # Ignore les options vides
                    category_text = option.text
                    if self.is_target_category(category_text):  # Application du filtre
                        categories.append({
                            'value': option.get_attribute("value"),  # Valeur pour sélection
                            'text': category_text  # Texte affiché
                        })
                        logger.info(f"Found target category: {category_text}")  # Log des catégories retenues
            
            # Récapitulatif des catégories identifiées
            logger.info(f"Found {len(categories)} target document categories (SAQ/PCI DSS): {[cat['text'] for cat in categories]}")
            return categories  # Retour de la liste filtrée

        except Exception as e:
            logger.error(f"Could not find document categories: {e}")  # Gestion d'erreur avec log
            return []  # Retour vide en cas d'échec

    def select_category_and_get_links(self, category_value, category_text):
        """Sélectionne une catégorie et extrait tous les liens PDF

        Méthode simplifiée pour extraction basique des liens de téléchargement
        """
        try:
            logger.info(f"Processing target category: {category_text}")  # Log de traitement

            # Sélection de la catégorie dans le dropdown
            category_dropdown = self.driver.find_element(By.ID, "document_category")
            select = Select(category_dropdown)
            select.select_by_value(category_value)  # Application de la sélection

            # Temporisation pour chargement du contenu dynamique
            time.sleep(3)  # Attente fixe pour stabilité

            # Attente conditionnelle de la présence des boutons de téléchargement
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='download_btn_']"))  # Sélecteur CSS générique
                )
            except Exception as e:
                logger.warning(f"No download buttons found for category {category_text}: {e}")  # Log d'alerte
                return []  # Retour vide si pas de boutons

            # Récupération du source HTML et parsing avec BeautifulSoup
            page_source = self.driver.page_source  # HTML complet de la page
            soup = BeautifulSoup(page_source, 'html.parser')  # Parser HTML pour navigation DOM

            # Recherche de tous les boutons de téléchargement par pattern d'ID
            download_buttons = soup.find_all('div', id=lambda x: x and x.startswith('download_btn_'))  # Filtre lambda sur ID
            logger.info(f"Found {len(download_buttons)} download buttons in {category_text}")  # Comptage des boutons

            pdf_links = []  # Accumulation des liens PDF trouvés
            for button in download_buttons:  # Itération sur chaque bouton
                link_elem = button.find('a', class_='download_doc')  # Recherche du lien de téléchargement
                if link_elem and link_elem.get('href'):  # Vérification présence href
                    href = link_elem['href']
                    # Validation que c'est bien un lien PDF
                    if href.lower().endswith('.pdf'):  # Filtre par extension
                        pdf_links.append({
                            'url': href,  # URL de téléchargement
                            'category': category_text  # Catégorie source
                        })
                        logger.info(f"Found PDF link in {category_text}: {href}")  # Log de découverte

            # Recherche complémentaire de liens PDF directs dans la page
            all_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))  # Filtre tous liens PDF
            for link in all_links:  # Itération sur les liens supplémentaires
                href = link['href']
                # Vérification anti-doublon
                if not any(pdf['url'] == href for pdf in pdf_links):  # Éviter les duplicatas
                    pdf_links.append({
                        'url': href,
                        'category': category_text
                    })
                    logger.info(f"Found additional PDF link in {category_text}: {href}")  # Log supplémentaire

            return pdf_links  # Retour de tous les liens découverts

        except Exception as e:
            logger.error(f"Error processing category {category_text}: {e}")  # Gestion d'erreur
            return []  # Retour vide en cas d'échec

    def select_category_and_get_links_enhanced(self, category_value, category_text):
        """Méthode avancée de mapping précis des documents avec versions et langues

        Analyse fine des documents avec support multi-langue (EN/FR/ES/DE/PT)
        et extraction des métadonnées complètes (nom, version, langue)
        """
        try:
            logger.info(f"Processing target category with enhanced precision: {category_text}")  # Log mode avancé

            # Sélection de catégorie avec mapping précis
            category_dropdown = self.driver.find_element(By.ID, "document_category")
            select = Select(category_dropdown)
            select.select_by_value(category_value)  # Application de la sélection ciblée

            # Attente du rechargement dynamique du contenu
            time.sleep(3)  # Stabilisation après sélection

            # Validation de la présence d'éléments téléchargeables
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='download_btn_']"))  # Attente des boutons
                )
            except Exception as e:
                logger.warning(f"No download buttons found for category {category_text}: {e}")
                return []  # Échec si pas de contenu

            # Extraction du DOM pour analyse structurelle
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')  # Parser pour analyse précise
            
            # Analyse structurelle des lignes de documents
            # Structure: span document_name + div version + div download button
            document_links = []  # Accumulation des métadonnées complètes

            # Identification de tous les noms de documents par classe CSS
            document_names = soup.find_all('span', class_='document_name')  # Sélecteur de noms
            logger.info(f"Found {len(document_names)} document names in {category_text}")  # Inventaire
            
            # Itération sur chaque document identifié
            for doc_span in document_names:
                try:
                    document_name = doc_span.text.strip()  # Extraction nom nettoyé
                    if not document_name:  # Skip si nom vide
                        continue
                    
                    # Navigation hiérarchique pour trouver le conteneur de ligne
                    row_container = doc_span.find_parent()  # Remontée DOM initiale
                    # Recherche du conteneur avec version_select_ (indicateur de ligne complète)
                    while row_container and not any(child.get('id', '').startswith('version_select_') for child in row_container.find_all()):
                        row_container = row_container.find_parent()  # Remontée continue

                    if not row_container:  # Échec de localisation structurelle
                        logger.warning(f"Could not find row container for document: {document_name}")
                        continue  # Skip ce document
                    
                    # Extraction des informations de version dans la ligne
                    version_div = row_container.find('div', id=lambda x: x and x.startswith('version_select_'))  # Localisation version
                    version_text = version_div.text.strip() if version_div else "N/A"  # Texte version ou fallback
                    
                    # Extraction de l'identifiant unique du document
                    doc_id = None
                    if version_div and version_div.get('id'):  # Si version div avec ID
                        doc_id = version_div['id'].replace('version_select_', '')  # Extraction suffix ID
                    
                    # Recherche des variantes linguistiques disponibles (EN/FR/ES/DE/PT)
                    language_variants = self.get_document_language_variants(row_container, doc_id, document_name, version_text, category_text)  # Délégation multi-langue
                    
                    # If no language variants found, fall back to the default download
                    if not language_variants:
                        # Find download button in this row
                        download_div = row_container.find('div', id=lambda x: x and x.startswith('download_btn_'))
                        if not download_div:
                            logger.warning(f"No download button found for document: {document_name}")
                            continue
                        
                        download_link = download_div.find('a', class_='download_doc')
                        if not download_link or not download_link.get('href'):
                            logger.warning(f"No download link found for document: {document_name}")
                            continue
                        
                        pdf_url = download_link['href']
                        if not pdf_url.lower().endswith('.pdf'):
                            logger.warning(f"Link is not a PDF for document: {document_name}")
                            continue
                        
                        document_info = {
                            'url': pdf_url,
                            'category': category_text,
                            'document_name': document_name,
                            'version': version_text,
                            'document_id': doc_id,
                            'language': 'EN',  # Default to English
                            'filename_hint': self.extract_filename_from_url(pdf_url)
                        }
                        
                        document_links.append(document_info)
                        logger.info(f"Mapped document: {document_name} (v{version_text}, EN) -> {pdf_url}")
                    else:
                        # Add all language variants
                        document_links.extend(language_variants)
                    
                except Exception as e:
                    logger.warning(f"Error processing document in {category_text}: {e}")
                    continue
            
            logger.info(f"Successfully mapped {len(document_links)} documents in {category_text}")
            return document_links
            
        except Exception as e:
            logger.error(f"Error in enhanced processing for category {category_text}: {e}")
            return []

    def get_document_language_variants(self, row_container, doc_id, document_name, version_text, category_text):
        """Get all language variants (English and French) for a document if available"""
        language_variants = []
        
        try:
            # Look for language dropdown in the row
            language_select = row_container.find('select', attrs={'data-doc_idx': doc_id})
            if not language_select:
                # Try to find any select element that might be the language selector
                select_elements = row_container.find_all('select')
                for select_elem in select_elements:
                    if select_elem.find('option', string=lambda text: text and 'French PDF' in text):
                        language_select = select_elem
                        break
            
            if language_select:
                logger.info(f"Found language dropdown for document: {document_name}")
                
                # Get all language options
                options = language_select.find_all('option')
                
                for option in options:
                    option_text = option.text.strip()
                    option_value = option.get('value', '')
                    
                    # Map option text to language codes
                    if 'English PDF' in option_text:
                        lang_code = 'EN'
                    elif 'French PDF' in option_text:
                        lang_code = 'FR'
                    elif 'Spanish PDF' in option_text:
                        lang_code = 'ES'
                    elif 'German PDF' in option_text:
                        lang_code = 'DE'
                    elif 'Portuguese PDF' in option_text:
                        lang_code = 'PT'
                    elif 'Chinese PDF' in option_text:
                        continue  # Skip Chinese for now
                    elif 'Japanese PDF' in option_text:
                        continue  # Skip Japanese for now
                    else:
                        continue  # Skip unknown languages
                    
                    # For each supported language, we need to determine the download URL
                    # Since the dropdown changes the download link dynamically via JavaScript,
                    # we'll store the dropdown info and handle the selection later
                    document_info = {
                        'url': None,  # Will be determined during download
                        'category': category_text,
                        'document_name': document_name,
                        'version': version_text,
                        'document_id': doc_id,
                        'language': lang_code,
                        'language_option_value': option_value,
                        'language_select_data_idx': language_select.get('data-doc_idx'),
                        'needs_language_selection': True,
                        'filename_hint': f"{document_name}_{lang_code}.pdf"
                    }
                    
                    language_variants.append(document_info)
                    logger.info(f"Added language variant: {document_name} (v{version_text}, {lang_code})")
                
            return language_variants
            
        except Exception as e:
            logger.warning(f"Error getting language variants for {document_name}: {e}")
            return []

    def get_all_pdf_links(self):
        """Get PDF links from SAQ and PCI DSS document categories with precise document identification"""
        logger.info("Loading document library page...")
        self.driver.get(self.base_url)
        
        # Wait for page to load completely
        time.sleep(5)
        
        all_pdf_links = []
        
        # Get filtered categories (SAQ and PCI DSS only)
        categories = self.get_document_categories()
        
        if not categories:
            logger.warning("No target categories found (SAQ/PCI DSS)")
            return []
        
        # Process each target category
        for category in categories:
            category_links = self.select_category_and_get_links_enhanced(category['value'], category['text'])
            all_pdf_links.extend(category_links)
            
            # Small delay between categories
            time.sleep(2)
        
        logger.info(f"Total PDF links found in target categories (SAQ/PCI DSS): {len(all_pdf_links)}")
        return all_pdf_links

    def download_pdf_enhanced(self, pdf_info, filename):
        """Enhanced PDF download with language selection support"""
        max_retries = 3
        retry_delay = 2
        
        # Check if this document needs language selection
        if pdf_info.get('needs_language_selection', False):
            return self.download_with_language_selection(pdf_info, filename)
        
        # Standard download for documents without language options
        url = pdf_info.get('url') if isinstance(pdf_info, dict) else pdf_info
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading {filename} (attempt {attempt + 1}/{max_retries})")
                
                # Use only navigation download method
                success = self.download_via_navigation(url, filename)
                if success:
                    return True
                        
            except Exception as e:
                logger.error(f"Error in attempt {attempt + 1} for {url}: {e}")
                
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Gradual backoff
                
        logger.error(f"Failed to download {filename} after {max_retries} attempts")
        return False

    def download_with_language_selection(self, pdf_info, filename):
        """Download PDF with specific language selection"""
        try:
            logger.info(f"Downloading {filename} with language selection: {pdf_info['language']}")
            
            # Navigate to the document library page
            self.driver.get(self.base_url)
            time.sleep(3)
            
            # Select the correct category
            category_dropdown = self.driver.find_element(By.ID, "document_category")
            select = Select(category_dropdown)
            
            # Find the correct category value - we need to map back from category text
            category_found = False
            for option in select.options:
                if option.text and self.is_target_category(option.text):
                    if pdf_info['category'] in option.text:
                        select.select_by_value(option.get_attribute("value"))
                        category_found = True
                        break
            
            if not category_found:
                logger.error(f"Could not find category for {pdf_info['category']}")
                return False
            
            # Wait for page to load
            time.sleep(3)
            
            # Find the language dropdown for this document
            doc_idx = pdf_info.get('language_select_data_idx') or pdf_info.get('document_id')
            if doc_idx:
                try:
                    # Try to find the language dropdown
                    language_dropdown = self.driver.find_element(By.CSS_SELECTOR, f"select[data-doc_idx='{doc_idx}']")
                except:
                    # Fallback: find any select with the right option value
                    try:
                        language_dropdown = self.driver.find_element(By.XPATH, f"//select[option[@value='{pdf_info['language_option_value']}']]")
                    except:
                        logger.error(f"Could not find language dropdown for document {pdf_info['document_name']}")
                        return False
                
                # Select the desired language
                select_lang = Select(language_dropdown)
                select_lang.select_by_value(pdf_info['language_option_value'])
                
                # Wait for the page to update the download link
                time.sleep(2)
                
                # Find and click the download button
                try:
                    download_btn = self.driver.find_element(By.CSS_SELECTOR, f"div[id='download_btn_{doc_idx}'] a.download_doc")
                    download_url = download_btn.get_attribute('href')
                    
                    if download_url:
                        logger.info(f"Found download URL for {pdf_info['language']}: {download_url}")
                        return self.download_via_navigation(download_url, filename)
                    else:
                        logger.error(f"No download URL found after language selection for {filename}")
                        return False
                        
                except Exception as e:
                    logger.error(f"Could not find download button after language selection: {e}")
                    return False
            else:
                logger.error(f"No document ID found for language selection: {pdf_info}")
                return False
                
        except Exception as e:
            logger.error(f"Error in language-specific download for {filename}: {e}")
            return False


    def download_via_navigation(self, url, filename):
        """Download by navigating to PDF URL directly"""
        try:
            logger.info(f"Attempting navigation download for {filename}")
            
            # Navigate to the PDF URL
            self.driver.get(url)
            time.sleep(5)
            
            # Check if we got the PDF content
            current_url = self.driver.current_url
            page_source = self.driver.page_source
            
            # If the URL is still the PDF URL and we have content
            if current_url == url or url in current_url:
                # Try to get the PDF content via requests with current cookies
                cookies = self.driver.get_cookies()
                session = requests.Session()
                
                for cookie in cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
                
                session.headers.update({
                    'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                    'Referer': self.base_url
                })
                
                response = session.get(current_url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 1000:  # Ensure we got actual content
                    filepath = os.path.join(self.download_dir, filename)
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    file_size = os.path.getsize(filepath)
                    logger.info(f"Successfully downloaded {filename} via navigation ({file_size} bytes)")
                    return True
                    
        except Exception as e:
            logger.warning(f"Navigation download failed: {e}")
        
        return False


    def extract_filename_from_url(self, url):
        """Extract filename from URL"""
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        
        # If no filename, create one from the URL
        if not filename or not filename.endswith('.pdf'):
            filename = f"document_{hash(url) % 10000}.pdf"
            
        # Clean filename
        filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).strip()
        return filename

    def run(self):
        """Main scraping function - only downloads SAQ and PCI DSS documents"""
        try:
            logger.info("Starting Enhanced PCI Standards document scraper (SAQ and PCI DSS only)...")
            
            # Setup selenium driver
            self.setup_driver()
            
            # Get PDF links from SAQ and PCI DSS categories only
            pdf_links = self.get_all_pdf_links()
            
            if not pdf_links:
                logger.error("No PDF links found in target categories (SAQ/PCI DSS)!")
                return
            
            # Download each PDF
            successful_downloads = 0
            for i, pdf_info in enumerate(pdf_links, 1):
                category = pdf_info.get('category', 'Unknown')
                document_name = pdf_info.get('document_name', 'Unknown')
                language = pdf_info.get('language', 'EN')
                
                # Create filename with language suffix
                base_name = pdf_info.get('filename_hint', self.extract_filename_from_url(pdf_info.get('url', '')))
                if not base_name or base_name == '':
                    base_name = f"{document_name}.pdf"
                
                # Ensure filename includes language
                if not base_name.lower().endswith(f'_{language.lower()}.pdf'):
                    name_part = base_name.replace('.pdf', '')
                    filename = f"{name_part}_{language}.pdf"
                else:
                    filename = base_name
                
                # Add category prefix to filename to avoid conflicts
                safe_category = "".join(c for c in category if c.isalnum() or c in (' ', '-', '_')).strip()
                if safe_category:
                    filename = f"{safe_category}_{filename}"
                
                # Clean filename
                filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).strip()
                
                logger.info(f"Processing {i}/{len(pdf_links)}: {filename} (Category: {category}, Language: {language})")
                
                # Check if file already exists
                filepath = os.path.join(self.download_dir, filename)
                if os.path.exists(filepath):
                    logger.info(f"File {filename} already exists, skipping...")
                    successful_downloads += 1
                    continue
                
                if self.download_pdf_enhanced(pdf_info, filename):
                    successful_downloads += 1
                
                # Random delay between downloads
                time.sleep(random.uniform(1, 3))
            
            logger.info(f"Enhanced scraping completed! Downloaded {successful_downloads}/{len(pdf_links)} files from SAQ and PCI DSS categories")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    scraper = PCIScraperEnhanced()
    scraper.run()