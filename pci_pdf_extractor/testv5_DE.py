#!/usr/bin/env python3
"""
Extracteur automatique d'exigences PCI DSS (Version 5 - Améliorée) - Version Allemande
Automatic PCI DSS Requirements Extractor (Version 5 - Improved) - German Version
Extrait toutes les exigences du document SAQ D v4.0.1 allemand
Format de sortie: {'req_num': '...', 'text': '...', 'tests': [...], 'guidance': '...'}
"""
import re
import json
import PyPDF2
from typing import List, Dict, Any, Tuple

class PCIRequirementsExtractor:
    """Classe principale pour extraire les exigences PCI DSS depuis un PDF allemand"""

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.requirements = []
        
        # Marqueurs pour identifier les sections en allemand
        self.test_indicators = ['• Datenflussdiagramme', '• Netzwerkkonfigurationen', '• Dokumentation', '• Verantwortliches Personal', '• NSC-Konfigurationen', '• untersuchen', '• beachten', '• befragen']
        self.applicability_marker = "Hinweise zur Anwendbarkeit"
        self.guidance_marker = "Leitfaden"

    def find_start_page(self) -> int:
        """Détecte automatiquement la page de début (contenant 1.1.1)"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()
                    # Chercher le pattern 1.1.1 au début d'une ligne
                    if re.search(r'^1\.1\.1\s+', page_text, re.MULTILINE):
                        print(f"Page de début détectée: {page_num + 1} (contient 1.1.1)")
                        return page_num
                        
        except Exception as e:
            print(f"Erreur lors de la recherche de la page de début: {e}")
            
        # Fallback vers la page 16 si pas trouvé
        print("Page de début non trouvée, utilisation de la page 16 par défaut")
        return 15
    
    def find_end_page(self) -> int:
        """Détecte automatiquement la page de fin (contenant la plus grande combinaison)"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                highest_requirement = ""
                end_page = len(pdf_reader.pages) - 1
                
                # Chercher la plus grande combinaison de numéros
                pattern = r'^(\d+\.\d+(?:\.\d+)*(?:\.\d+)*)\s+'
                
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()
                    matches = re.findall(pattern, page_text, re.MULTILINE)
                    
                    for match in matches:
                        # Vérifier que c'est un numéro d'exigence PCI valide (1-12)
                        parts = match.split('.')
                        if len(parts) >= 2:
                            main_num = int(parts[0])
                            if 1 <= main_num <= 12:
                                # Comparer avec le plus haut trouvé jusqu'à présent
                                if self._is_higher_requirement(match, highest_requirement):
                                    highest_requirement = match
                                    end_page = page_num
                
                if highest_requirement:
                    print(f"Page de fin détectée: {end_page + 1} (dernière exigence: {highest_requirement})")
                    return end_page
                    
        except Exception as e:
            print(f"Erreur lors de la recherche de la page de fin: {e}")
            
        # Fallback vers la page 129 si pas trouvé
        print("Page de fin non trouvée, utilisation de la page 129 par défaut")
        return 128
    
    def _is_higher_requirement(self, req1: str, req2: str) -> bool:
        """Compare deux numéros d'exigence pour déterminer lequel est le plus haut"""
        if not req2:
            return True
            
        parts1 = [int(x) for x in req1.split('.')]
        parts2 = [int(x) for x in req2.split('.')]
        
        # Égaliser les longueurs avec des zéros
        max_len = max(len(parts1), len(parts2))
        parts1.extend([0] * (max_len - len(parts1)))
        parts2.extend([0] * (max_len - len(parts2)))
        
        return parts1 > parts2

    def read_pdf_content(self) -> str:
        """Lit le contenu du PDF et retourne le texte complet avec détection automatique des pages"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                
                # Détection automatique des pages de début et fin
                start_page = self.find_start_page()
                end_page = self.find_end_page()
                
                # S'assurer que end_page est après start_page
                if end_page <= start_page:
                    end_page = min(len(pdf_reader.pages) - 1, start_page + 100)
                    print(f"Ajustement de la page de fin à {end_page + 1}")
                
                print(f"Extraction des pages {start_page + 1} à {end_page + 1}")
                
                for page_num in range(start_page, end_page + 1):
                    if page_num < len(pdf_reader.pages):
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text() + "\n"
                        
            return text
        except Exception as e:
            print(f"Erreur lors de la lecture du PDF: {e}")
            return ""

    def clean_text(self, text: str) -> str:
        """Nettoie le texte extrait du PDF en supprimant les artefacts allemands"""
        # Remove comprehensive copyright and page artifacts
        text = re.sub(r'PCI DSS v[\d.]+\s+SAQ D für Händler.*?Oktober \d+', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'SAQ D von PCI DSS v[\d.]+.*?Seite \d+.*?(?:Implementiert|Nicht Implementiert)', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'© 2006[−-]\d+\s+PCI Security Standards Council,\s+LLC\.\s+Alle Rechte vorbehalten\.\s+Seite\s+\w+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?LLC.*?Alle Rechte vorbehalten\.?', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Oktober \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'♦\s*Siehe.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\(Eine Antwort auswählen.*?\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Abschnitt \d+ :', '', text, flags=re.IGNORECASE)
        
        # Clean response tables
        text = re.sub(r'Implementiert\s+Implementiert mit CCW\s+Nicht Zutreffend\s+Nicht Getestet\s+Nicht Implementiert', '', text, flags=re.IGNORECASE)
        text = re.sub(r'mit CCW\s+Nicht Zutreffend\s+Nicht Getestet\s+Nicht Implementiert', '', text, flags=re.IGNORECASE)
        text = re.sub(r'mit CCW Nicht Zutreffend Nicht Getestet Nicht.*', '', text, flags=re.IGNORECASE)
        
        # Additional cleaning patterns
        text = re.sub(r'PCI DSS v[\d.]+.*?Self\s*-\s*Assessment\s+Questionnaire.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Abschnitt\s+\d+\s*:\s*Self\s*-\s*Assessment\s+Questionnaire.*?(?=\n)', '', text, flags=re.IGNORECASE)
        
        # Clean isolated Page references
        text = re.sub(r'Seite \d+\s*$', '', text, flags=re.MULTILINE | re.IGNORECASE)
        
        # Replace multiple line breaks with single ones
        text = re.sub(r'\n\s*\n', '\n\n', text)
        # Remove spaces at beginning/end of lines
        lines = [line.strip() for line in text.splitlines()]
        return "\n".join(lines)

    def is_requirement_number(self, line: str) -> str:
        """Vérifie si une ligne commence par un numéro d'exigence valide"""
        # Pattern plus précis pour les numéros d'exigence
        pattern = r'^(\d+\.\d+(?:\.\d+)*(?:\.\d+)*)\s+'
        match = re.match(pattern, line.strip())
        if match:
            req_num = match.group(1)
            # Éviter les numéros de page ou de version et valider la plage
            parts = req_num.split('.')
            if len(parts) >= 2:
                main_num = int(parts[0])
                # Les exigences PCI vont de 1 à 12
                if 1 <= main_num <= 12:
                    return req_num
        return ""

    def is_test_line(self, line: str) -> bool:
        """Vérifie si une ligne est une ligne de test"""
        line_clean = line.strip()
        # Check exact matches first
        if any(line_clean.startswith(indicator) for indicator in self.test_indicators):
            return True
        # Check for common test patterns without bullet
        test_verbs = ['untersuchen', 'beachten', 'befragen', 'prüfen', 'beobachten', 'überprüfen', 'inspizieren', 'verifizieren']
        # Check if line starts with bullet + verb pattern
        import re
        pattern = r'^[•\-\*]\s*(' + '|'.join(test_verbs) + r')\b'
        return bool(re.match(pattern, line_clean, re.IGNORECASE))

    def extract_requirement_text(self, line: str, req_num: str) -> str:
        """Extrait le texte de l'exigence en supprimant le numéro"""
        # Trouver la position après le numéro d'exigence
        pattern = rf'^{re.escape(req_num)}\s+'
        cleaned_line = re.sub(pattern, '', line.strip())
        return cleaned_line

    def parse_requirements(self, content: str) -> List[Dict[str, Any]]:
        """Parse les exigences du contenu texte"""
        requirements = []
        lines = content.splitlines()
        i = 0
        current_req = None

        while i < len(lines):
            line = lines[i].strip()
            
            if not line:  # Ignorer les lignes vides
                i += 1
                continue

            # Vérifier si c'est le début d'une nouvelle exigence
            req_num = self.is_requirement_number(line)
            if req_num:
                # Sauvegarder l'exigence précédente si elle existe
                if current_req:
                    self._finalize_requirement(current_req)
                    if not any(req['req_num'] == current_req['req_num'] for req in requirements):
                        requirements.append(current_req)

                # Initialiser une nouvelle exigence
                req_text = self.extract_requirement_text(line, req_num)
                current_req = {
                    'req_num': req_num,
                    'text': req_text,
                    'tests': [],
                    'guidance': ''
                }
                i += 1
                continue

            # Si une exigence est en cours de traitement
            if current_req:
                # Vérifier si c'est une ligne de test 
                if self.is_test_line(line):
                    # Extraire le test complet en préservant le verbe d'action
                    test_text = line
                    # Nettoyer la puce mais garder le verbe
                    test_text = re.sub(r'^•\s*', '', test_text).strip()
                    
                    # Rassembler les lignes de continuation pour ce test
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue
                        # Arrêter si on trouve une nouvelle exigence, un nouveau test, ou une section spéciale
                        if (self.is_requirement_number(next_line) or 
                            self.is_test_line(next_line) or
                            next_line.startswith(self.applicability_marker) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break
                        # Ajouter la continuation au test en cours
                        test_text += " " + next_line
                        j += 1
                    
                    # Nettoyer le test des artefacts
                    test_text = self._clean_test_text(test_text)
                    if test_text and len(test_text) > 10:  # Seulement les tests significatifs
                        current_req['tests'].append(test_text)
                    
                    i = j
                    continue

                # Vérifier si c'est la section Anwendungshinweise 
                elif line.startswith(self.applicability_marker) or line.startswith('Hinweise zur Anwendbarkeit'):
                    # Extraire le contenu des notes d'applicabilité dans le champ guidance
                    guidance_text = line[len(self.applicability_marker):].strip(': ')
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue
                        if (self.is_requirement_number(next_line) or 
                            self.is_test_line(next_line) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break
                        guidance_text += " " + next_line
                        j += 1
                    
                    # Nettoyer et stocker dans guidance
                    current_req['guidance'] = self._clean_guidance_text(guidance_text)
                    i = j
                    continue

                # Vérifier si c'est la section Leitfaden
                elif line.startswith(self.guidance_marker) or line.startswith('Leitfaden'):
                    guidance_text = line[len(self.guidance_marker):].strip(': ')
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue
                        if (self.is_requirement_number(next_line) or 
                            self.is_test_line(next_line) or
                            next_line.startswith(self.applicability_marker) or
                            self._should_ignore_line(next_line)):
                            break
                        guidance_text += " " + next_line
                        j += 1
                    
                    current_req['guidance'] = self._clean_guidance_text(guidance_text)
                    i = j
                    continue

                # Vérifier si c'est du contenu à ignorer
                elif self._should_ignore_line(line):
                    i += 1
                    continue

                # Sinon, c'est du texte appartenant à l'exigence principale
                else:
                    # Vérifier si le texte contient des tests cachés
                    # ET gérer les tests sur plusieurs lignes
                    cleaned_line, j = self._extract_tests_from_text_line_multiline(line, current_req, lines, i)
                    
                    # Si on a traité des lignes supplémentaires pour des tests multi-lignes
                    if j > i:
                        i = j
                        continue
                    
                    # Ajouter au texte principal seulement si ce n'est pas redondant ou parasite
                    if cleaned_line and self._is_valid_text_line(cleaned_line, current_req['text']):
                        if current_req['text']:
                            current_req['text'] += " " + cleaned_line
                        else:
                            current_req['text'] = cleaned_line

            i += 1

        # Sauvegarder la dernière exigence
        if current_req:
            self._finalize_requirement(current_req)
            if not any(req['req_num'] == current_req['req_num'] for req in requirements):
                requirements.append(current_req)

        return requirements

    def _extract_tests_from_text_line_multiline(self, line: str, current_req: Dict[str, Any], all_lines: List[str], current_index: int) -> Tuple[str, int]:
        """Extrait les tests cachés dans une ligne de texte et gère les tests multi-lignes"""
        remaining_text = line
        processed_lines = current_index
        
        # Trouver tous les tests dans la ligne
        for indicator in self.test_indicators:
            verb = indicator[2:]  # Enlever "• " pour avoir juste "Prüfen", "Beobachten", etc.
            pattern = rf'•\s*{re.escape(verb)}[^•]*'
            matches = list(re.finditer(pattern, remaining_text, re.IGNORECASE))
            
            for match in reversed(matches):  # Traiter de droite à gauche pour préserver les positions
                test_text = match.group(0)
                test_text = re.sub(r'^•\s*', '', test_text).strip()
                
                # Vérifier si le test semble incomplet (très court ou se termine abruptement)
                # et rassembler les lignes suivantes si nécessaire
                if len(test_text) < 30 or not test_text.endswith('.'):
                    # Rassembler les lignes suivantes pour ce test
                    j = current_index + 1
                    while j < len(all_lines):
                        next_line = all_lines[j].strip()
                        if not next_line:
                            j += 1
                            continue
                        
                        # Arrêter si on trouve une nouvelle exigence, un nouveau test, ou une section spéciale
                        if (self.is_requirement_number(next_line) or 
                            self.is_test_line(next_line) or
                            next_line.startswith(self.applicability_marker) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break
                        
                        # Ajouter la continuation au test en cours
                        test_text += " " + next_line
                        processed_lines = j  # Marquer cette ligne comme traitée
                        
                        # Si on a une phrase complète (se termine par . ! ou ?), on peut arrêter
                        if next_line.endswith('.') or next_line.endswith('!') or next_line.endswith('?'):
                            break
                        
                        j += 1
                
                # Nettoyer le test des artefacts
                test_text = self._clean_test_text(test_text)
                
                if test_text and len(test_text) > 10:
                    # Ajouter le test s'il n'existe pas déjà
                    if test_text not in current_req['tests']:
                        current_req['tests'].append(test_text)
                    
                    # Supprimer le test du texte restant
                    remaining_text = remaining_text[:match.start()] + ' ' + remaining_text[match.end():]
        
        # Nettoyer le texte restant
        remaining_text = re.sub(r'\s+', ' ', remaining_text).strip()
        return remaining_text, processed_lines

    def _extract_tests_from_text_line(self, line: str, current_req: Dict[str, Any]) -> str:
        """Extrait les tests cachés dans une ligne de texte et les ajoute à tests[] (version simple)"""
        remaining_text = line
        
        # Trouver tous les tests dans la ligne
        for indicator in self.test_indicators:
            verb = indicator[2:]  # Enlever "• " pour avoir juste "Prüfen", "Beobachten", etc.
            pattern = rf'•\s*{re.escape(verb)}[^•]*'
            matches = list(re.finditer(pattern, remaining_text, re.IGNORECASE))
            
            for match in reversed(matches):  # Traiter de droite à gauche pour préserver les positions
                test_text = match.group(0)
                test_text = re.sub(r'^•\s*', '', test_text).strip()
                test_text = self._clean_test_text(test_text)
                
                if test_text and len(test_text) > 10:
                    # Ajouter le test s'il n'existe pas déjà
                    if test_text not in current_req['tests']:
                        current_req['tests'].append(test_text)
                    
                    # Supprimer le test du texte restant
                    remaining_text = remaining_text[:match.start()] + ' ' + remaining_text[match.end():]
        
        # Nettoyer le texte restant
        remaining_text = re.sub(r'\s+', ' ', remaining_text).strip()
        return remaining_text

    def _clean_test_text(self, text: str) -> str:
        """Nettoie le texte d'un test en supprimant les artefacts allemands"""
        # Remove layout artifacts - more comprehensive patterns
        text = re.sub(r'SAQ D von PCI DSS.*?Seite \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?LLC.*?Alle Rechte vorbehalten.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI Security Standards Council.*?LLC.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Seite \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Implementiert.*?Nicht Implementiert', '', text, flags=re.IGNORECASE)
        text = re.sub(r'♦\s*Siehe.*', '', text, flags=re.IGNORECASE)
        
        # Remove response table artifacts
        text = re.sub(r'mit CCW Nicht Zutreffend Nicht Getestet Nicht.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Implementiert\s+Implementiert mit CCW\s+Nicht Zutreffend\s+Nicht Getestet\s+Nicht Implementiert', '', text, flags=re.IGNORECASE)
        text = re.sub(r'(Implementiert|Nicht Implementiert|Nicht Zutreffend|Nicht Getestet|CCW)(\s+(Implementiert|Nicht Implementiert|Nicht Zutreffend|Nicht Getestet|CCW))*', '', text, flags=re.IGNORECASE)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _clean_guidance_text(self, text: str) -> str:
        """Nettoie le texte de guidance en supprimant les artefacts"""
        # Remove similar artifacts - more comprehensive patterns
        text = re.sub(r'SAQ D von PCI DSS.*?Seite \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?LLC.*?Alle Rechte vorbehalten.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI Security Standards Council.*?LLC.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Seite \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Implementiert.*?Nicht Implementiert', '', text, flags=re.IGNORECASE)
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _is_valid_text_line(self, line: str, current_text: str) -> bool:
        """Vérifie si une ligne est valide pour être ajoutée au texte principal"""
        # Éviter les répétitions
        if line in current_text:
            return False
        
        # Éviter les lignes trop courtes ou qui semblent être des artefacts
        if len(line) < 3:
            return False
            
        # Éviter les lignes qui commencent par des puces non-test
        if line.startswith('•') and not self.is_test_line('• ' + line[1:]):
            return True
            
        return True

    def _should_ignore_line(self, line: str) -> bool:
        """Détermine si une ligne doit être ignorée (patterns allemands)"""
        ignore_patterns = [
            r'^SAQ D von PCI DSS',
            r'^© 2006[−-]\d+',
            r'^Seite \d+',
            r'^Oktober 2024',
            r'^PCI DSS-Anforderung',
            r'^Testverfahren',
            r'^Antwort',
            r'^Implementiert',
            r'^Nicht Implementiert',
            r'^Nicht Zutreffend',
            r'^Nicht Getestet',
            r'^♦ Siehe',
            r'^\(Eine Antwort auswählen',
            r'^Abschnitt \d+',
            r'^Alle Rechte vorbehalten',
            r'^LLC\.',
            r'^PCI Security Standards Council',
            r'^Self\s*-\s*Assessment\s+Questionnaire',
            r'^PCI DSS v[\d.]+',
            r'© 2006[−-]\d+.*PCI Security Standards Council.*LLC.*Alle Rechte vorbehalten.*Seite \d+',
        ]
        
        line_lower = line.lower()
        for pattern in ignore_patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return True
                
        # Check if line contains copyright artifacts anywhere
        if re.search(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten', line, re.IGNORECASE):
            return True
            
        # Ignorer les lignes très courtes qui sont probablement du bruit
        if len(line.strip()) <= 2:
            return True
            
        return False

    def _remove_response_artifacts(self, text: str) -> str:
        """Supprime les artefacts de cases de réponse du questionnaire allemand"""
        # Remove all variations of response checkboxes and copyright/page artifacts
        patterns_to_remove = [
            r'mit CCW Nicht Zutreffend Nicht Getestet Nicht.*?(?=\n|$)',
            r'Implementiert\s+Implementiert mit CCW\s+Nicht Zutreffend\s+Nicht Getestet\s+Nicht Implementiert',
            r'mit CCW\s+Nicht Zutreffend\s+Nicht Getestet\s+Nicht Implementiert',
            r'Implementiert.*?Nicht Implementiert.*?(?=\n|$)',
            r'(Implementiert|Nicht Implementiert|Nicht Zutreffend|Nicht Getestet|CCW)(\s+(Implementiert|Nicht Implementiert|Nicht Zutreffend|Nicht Getestet|CCW))+',
            r'♦\s*Siehe.*?(?=\n|$)',
            r'\(Eine Antwort auswählen.*?\)',
            r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+',
            r'© 2006[−-]\d+.*?LLC.*?Alle Rechte vorbehalten.*',
            r'PCI Security Standards Council.*?LLC.*',
            r'Alle Rechte vorbehalten.*?Seite \d+',
            r'Seite \d+[^\w]*$',
        ]
        
        for pattern in patterns_to_remove:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _finalize_requirement(self, req: Dict[str, Any]):
        """Nettoie et finalise une exigence avant de la sauvegarder"""
        # Extraire les tests restants du texte principal
        text_remaining = self._extract_tests_from_text_line(req['text'], req)
        req['text'] = text_remaining
        
        # Supprimer les artefacts de cases de réponse du texte principal
        req['text'] = self._remove_response_artifacts(req['text'])
        
        # Nettoyer le texte principal
        req['text'] = req['text'].strip()
        req['text'] = re.sub(r'\s+', ' ', req['text'])  # Normaliser les espaces
        
        # Nettoyer les tests et supprimer les doublons
        cleaned_tests = []
        for test in req['tests']:
            test_clean = self._remove_response_artifacts(test)
            test_clean = test_clean.strip()
            test_clean = re.sub(r'\s+', ' ', test_clean)
            if test_clean and test_clean not in cleaned_tests and len(test_clean) > 10:
                cleaned_tests.append(test_clean)
        req['tests'] = cleaned_tests
        
        # Nettoyer le guidance
        req['guidance'] = self._remove_response_artifacts(req['guidance'])
        req['guidance'] = req['guidance'].strip()
        req['guidance'] = re.sub(r'\s+', ' ', req['guidance'])

    def extract_all_requirements(self) -> List[Dict[str, Any]]:
        """Extrait toutes les exigences du PDF"""
        print("Lecture du PDF...")
        raw_text = self.read_pdf_content()
        if not raw_text:
            print("Échec de la lecture du PDF.")
            return []

        print("Nettoyage du texte...")
        clean_text = self.clean_text(raw_text)

        print("Parsing des exigences...")
        self.requirements = self.parse_requirements(clean_text)
        return self.requirements

    def print_summary(self):
        """Affiche un résumé des exigences extraites"""
        print(f"\nRésumé de l'extraction:")
        print(f"Nombre total d'exigences extraites: {len(self.requirements)}")
        if self.requirements:
            print(f"Première exigence: {self.requirements[0]['req_num']}")
            print(f"Dernière exigence: {self.requirements[-1]['req_num']}")
            
            # Statistiques
            with_tests = sum(1 for req in self.requirements if req['tests'])
            with_guidance = sum(1 for req in self.requirements if req['guidance'])
            total_tests = sum(len(req['tests']) for req in self.requirements)
            
            print(f"Exigences avec tests: {with_tests}")
            print(f"Exigences avec guidance: {with_guidance}")
            print(f"Total des tests extraits: {total_tests}")

    def save_to_json(self, output_file: str = "pci_requirements_v5_DE.json"):
        """Sauvegarde les exigences au format JSON"""
        # Trier par numéro d'exigence
        def sort_key(req):
            parts = [int(x) for x in req['req_num'].split('.')]
            # Pad with zeros to ensure consistent sorting
            while len(parts) < 4:
                parts.append(0)
            return parts
        
        sorted_requirements = sorted(self.requirements, key=sort_key)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sorted_requirements, f, ensure_ascii=False, indent=2)
        print("=" * 60)
        print(f"Exigences sauvegardées dans {output_file}")

    def save_to_csv(self, output_file: str = "pci_requirements_v5_DE.csv"):
        """Sauvegarde les exigences au format CSV avec structure simplifiée"""
        import csv
        
        # Trier par numéro d'exigence
        def sort_key(req):
            parts = [int(x) for x in req['req_num'].split('.')]
            while len(parts) < 4:
                parts.append(0)
            return parts
        
        sorted_requirements = sorted(self.requirements, key=sort_key)
        
        # Préparer les données pour CSV avec structure simplifiée
        csv_data = []
        for req in sorted_requirements:
            # Convertir les tests en chaîne séparée par des points-virgules
            tests_str = " ; ".join(req['tests']) if req['tests'] else ""
            
            csv_row = {
                'req_num': req['req_num'],
                'text': req['text'],
                'tests': tests_str,
                'guidance': req['guidance']
            }
            csv_data.append(csv_row)
        
        # Sauvegarder en CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['req_num', 'text', 'tests', 'guidance']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(csv_data)
        
        print("=" * 60)
        print(f"Exigences sauvegardées dans {output_file}")
        print(f"Structure CSV: {len(csv_data)} exigences avec colonnes simplifiées")

def main():
    pdf_path = "PCI-DSS-v4-0-1-SAQ-D-Merchant-DE.pdf"
    print("EXTRACTEUR PCI DSS ALLEMAND")
    print("=" * 60)
    
    extractor = PCIRequirementsExtractor(pdf_path)
    requirements = extractor.extract_all_requirements()

    if requirements:
        extractor.print_summary()
        extractor.save_to_csv("pci_requirements_v5_DE.csv")
        extractor.save_to_json("pci_requirements_v5_DE.json")  # Garde aussi JSON pour compatibilité
    else:
        print("Aucune exigence n'a pu être extraite.")

if __name__ == "__main__":
    main()