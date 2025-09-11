#!/usr/bin/env python3
"""
PCI Security Standards Document Scraper - Enhanced Version
Downloads only SAQ and PCI DSS PDFs with improved 403 bypass techniques
"""

import os
import time
import requests
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium_stealth import stealth
from bs4 import BeautifulSoup
import logging
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PCIScraperEnhanced:
    def __init__(self, download_dir="downloads"):
        self.download_dir = download_dir
        self.base_url = "https://www.pcisecuritystandards.org/document_library/"
        self.session = requests.Session()
        self.driver = None
        
        # Define the categories we want to download (SAQ and PCI DSS only)
        self.target_categories = [
            'SAQ',
            'PCI DSS',
            'Self-Assessment Questionnaire',
            'Data Security Standard'
        ]
        
        # Create download directory
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Enhanced headers rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        # Setup requests session with enhanced headers
        self.update_session_headers()

    def update_session_headers(self):
        """Update session headers with random user agent"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'max-age=0'
        })

    def setup_driver(self):
        """Setup Chrome driver with enhanced stealth configuration"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Random user agent for driver
        user_agent = random.choice(self.user_agents)
        chrome_options.add_argument(f"--user-agent={user_agent}")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Apply enhanced stealth
        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True)
        
        # Remove webdriver properties
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")

    def is_target_category(self, category_text):
        """Check if a category is one we want to download (SAQ or PCI DSS)"""
        category_lower = category_text.lower()
        
        target_keywords = [
            'saq',
            'self-assessment questionnaire',
            'pci dss',
            'data security standard',
            'pci-dss'
        ]
        
        for keyword in target_keywords:
            if keyword in category_lower:
                return True
        
        return False

    def get_document_categories(self):
        """Get all available document categories from the page, filtered for SAQ and PCI DSS"""
        try:
            # Wait for the category dropdown to be present
            category_dropdown = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "document_category"))
            )
            
            select = Select(category_dropdown)
            categories = []
            
            for option in select.options:
                if option.get_attribute("value"):  # Skip empty options
                    category_text = option.text
                    if self.is_target_category(category_text):
                        categories.append({
                            'value': option.get_attribute("value"),
                            'text': category_text
                        })
                        logger.info(f"Found target category: {category_text}")
            
            logger.info(f"Found {len(categories)} target document categories (SAQ/PCI DSS): {[cat['text'] for cat in categories]}")
            return categories
            
        except Exception as e:
            logger.error(f"Could not find document categories: {e}")
            return []

    def select_category_and_get_links(self, category_value, category_text):
        """Select a specific category and get all PDF links for that category"""
        try:
            logger.info(f"Processing target category: {category_text}")
            
            # Select the category
            category_dropdown = self.driver.find_element(By.ID, "document_category")
            select = Select(category_dropdown)
            select.select_by_value(category_value)
            
            # Wait for the page to load new content
            time.sleep(3)
            
            # Wait for download buttons to be present
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='download_btn_']"))
                )
            except Exception as e:
                logger.warning(f"No download buttons found for category {category_text}: {e}")
                return []
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Find all download button divs
            download_buttons = soup.find_all('div', id=lambda x: x and x.startswith('download_btn_'))
            logger.info(f"Found {len(download_buttons)} download buttons in {category_text}")
            
            pdf_links = []
            for button in download_buttons:
                link_elem = button.find('a', class_='download_doc')
                if link_elem and link_elem.get('href'):
                    href = link_elem['href']
                    # Ensure it's a PDF link
                    if href.lower().endswith('.pdf'):
                        pdf_links.append({
                            'url': href,
                            'category': category_text
                        })
                        logger.info(f"Found PDF link in {category_text}: {href}")
            
            # Also check for any direct PDF links in the page
            all_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
            for link in all_links:
                href = link['href']
                # Check if we already have this link
                if not any(pdf['url'] == href for pdf in pdf_links):
                    pdf_links.append({
                        'url': href,
                        'category': category_text
                    })
                    logger.info(f"Found additional PDF link in {category_text}: {href}")
            
            return pdf_links
            
        except Exception as e:
            logger.error(f"Error processing category {category_text}: {e}")
            return []

    def select_category_and_get_links_enhanced(self, category_value, category_text):
        """Enhanced method that precisely maps documents with their versions and download links for both EN and FR"""
        try:
            logger.info(f"Processing target category with enhanced precision: {category_text}")
            
            # Select the category
            category_dropdown = self.driver.find_element(By.ID, "document_category")
            select = Select(category_dropdown)
            select.select_by_value(category_value)
            
            # Wait for the page to load new content
            time.sleep(3)
            
            # Wait for download buttons to be present
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='download_btn_']"))
                )
            except Exception as e:
                logger.warning(f"No download buttons found for category {category_text}: {e}")
                return []
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Find document rows using the structure provided by the user
            # Each document has: document_name span, version div, and download button div
            document_links = []
            
            # Find all document name spans
            document_names = soup.find_all('span', class_='document_name')
            logger.info(f"Found {len(document_names)} document names in {category_text}")
            
            for doc_span in document_names:
                try:
                    document_name = doc_span.text.strip()
                    if not document_name:
                        continue
                    
                    # Find the parent row container
                    row_container = doc_span.find_parent()
                    while row_container and not any(child.get('id', '').startswith('version_select_') for child in row_container.find_all()):
                        row_container = row_container.find_parent()
                    
                    if not row_container:
                        logger.warning(f"Could not find row container for document: {document_name}")
                        continue
                    
                    # Find version info in this row
                    version_div = row_container.find('div', id=lambda x: x and x.startswith('version_select_'))
                    version_text = version_div.text.strip() if version_div else "N/A"
                    
                    # Extract the unique ID from the version or download div
                    doc_id = None
                    if version_div and version_div.get('id'):
                        doc_id = version_div['id'].replace('version_select_', '')
                    
                    # Get both English and French versions if language dropdown is available
                    language_variants = self.get_document_language_variants(row_container, doc_id, document_name, version_text, category_text)
                    
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