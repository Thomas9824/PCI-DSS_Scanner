#!/usr/bin/env python3
"""
Automatischer PCI DSS Anforderungsextraktor (Version 5 - Verbessert) - Deutsche Version
Automatic PCI DSS Requirements Extractor (Version 5 - Improved) - German Version
Extrahiert alle Anforderungen aus SAQ D v4.0.1 Dokument
Ausgabeformat: {'req_num': '...', 'text': '...', 'tests': [...], 'guidance': '...'}
"""
import re
import json
import PyPDF2
from typing import List, Dict, Any, Tuple

class PCIRequirementsExtractor:
    """Hauptklasse zum Extrahieren von PCI DSS Anforderungen aus einer PDF"""

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.requirements = []
        
        # Markierungen zur Identifizierung von Abschnitten auf Deutsch
        self.test_indicators = ['• Untersuchen', '• Befragen', '• Prüfen', '• Überprüfen', '• Bewerten', '• Beobachten']
        self.applicability_marker = "Hinweise zur Anwendbarkeit"
        self.guidance_marker = "Leitfaden"

    def find_start_page(self) -> int:
        """Erkennt automatisch die Startseite (enthält 1.1.1)"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()
                    # Look for pattern 1.1.1 at the beginning of a line
                    if re.search(r'^1\.1\.1\s+', page_text, re.MULTILINE):
                        print(f"Startseite erkannt: {page_num + 1} (enthält 1.1.1)")
                        return page_num
                        
        except Exception as e:
            print(f"Fehler bei der Suche nach Startseite: {e}")
            
        # Fallback auf Seite 16 wenn nicht gefunden
        print("Startseite nicht gefunden, verwende Seite 16 als Standard")
        return 15
    
    def find_end_page(self) -> int:
        """Automatically detects the end page (containing the highest combination)"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                highest_requirement = ""
                end_page = len(pdf_reader.pages) - 1
                
                # Look for the highest number combination
                pattern = r'^(\d+\.\d+(?:\.\d+)*(?:\.\d+)*)\s+'
                
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()
                    matches = re.findall(pattern, page_text, re.MULTILINE)
                    
                    for match in matches:
                        # Verify it's a valid PCI requirement number (1-12)
                        parts = match.split('.')
                        if len(parts) >= 2:
                            main_num = int(parts[0])
                            if 1 <= main_num <= 12:
                                # Compare with the highest found so far
                                if self._is_higher_requirement(match, highest_requirement):
                                    highest_requirement = match
                                    end_page = page_num
                
                if highest_requirement:
                    print(f"End page detected: {end_page + 1} (last requirement: {highest_requirement})")
                    return end_page
                    
        except Exception as e:
            print(f"Error searching for end page: {e}")
            
        # Fallback to page 129 if not found
        print("End page not found, using page 129 as default")
        return 128
    
    def _is_higher_requirement(self, req1: str, req2: str) -> bool:
        """Compares two requirement numbers to determine which is higher"""
        if not req2:
            return True
            
        parts1 = [int(x) for x in req1.split('.')]
        parts2 = [int(x) for x in req2.split('.')]
        
        # Equalize lengths with zeros
        max_len = max(len(parts1), len(parts2))
        parts1.extend([0] * (max_len - len(parts1)))
        parts2.extend([0] * (max_len - len(parts2)))
        
        return parts1 > parts2

    def read_pdf_content(self) -> str:
        """Reads PDF content and returns complete text with automatic page detection"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                
                # Automatic detection of start and end pages
                start_page = self.find_start_page()
                end_page = self.find_end_page()
                
                # Ensure end_page is after start_page
                if end_page <= start_page:
                    end_page = min(len(pdf_reader.pages) - 1, start_page + 100)
                    print(f"Adjusting end page to {end_page + 1}")
                
                print(f"Extracting pages {start_page + 1} to {end_page + 1}")
                
                for page_num in range(start_page, end_page + 1):
                    if page_num < len(pdf_reader.pages):
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text() + "\n"
                        
            return text
        except Exception as e:
            print(f"Error reading PDF: {e}")
            return ""

    def clean_text(self, text: str) -> str:
        """Reinigt extrahierten PDF-Text durch Entfernen von deutschen Artefakten"""
        # Entferne umfassende Copyright- und Seitenartefakte
        text = re.sub(r'PCI DSS v[\d.]+\s+SAQ D für Händler.*?Oktober \d+', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'SAQ D von PCI DSS v[\d.]+.*?Seite \d+.*?(?:Erfüllt|Nicht erfüllt)', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'© 2006[−-]\d+\s+PCI Security Standards Council,\s+LLC\.\s+Alle Rechte vorbehalten\.\s+Seite\s+\w+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+\s+PCI Security Standards Council,\s+LLC\.\s+Alle Rechte vorbehalten\.\s+Seite\s+\d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?LLC.*?Alle Rechte vorbehalten\.?', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI Security Standards Council.*?LLC.*?Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Alle Rechte vorbehalten.*?Seite \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Oktober \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'♦\s*Informationen zu diesen.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Anforderung Erwartetes Testen Antwort♦.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'© 2006\s*[−-]\s*\d+\s+PCI DSS\s*[−-]\s*Anforderung\s+Erwartetes\s+Testen\s+Antwort♦.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\(Eine Antwort für jede Anforderung ankreuzen.*?\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\(Eine Antwort ankreuzen.*?\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Abschnitt \d+ :', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Testverfahren\s*$', '', text, flags=re.IGNORECASE | re.MULTILINE)
        
        # Reinige Antworttabellen
        text = re.sub(r'Vorhanden\s+Vorhanden\s+mit CCW\s+Nicht\s+Anwendbar\s+Nicht\s+Getestet\s+Nicht\s+Vorhanden', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Erfüllt\s+Erfüllt mit CCW\s+Nicht anwendbar\s+Nicht getestet\s+Nicht erfüllt', '', text, flags=re.IGNORECASE)
        text = re.sub(r'mit CCW\s+Nicht anwendbar\s+Nicht getestet\s+Nicht erfüllt', '', text, flags=re.IGNORECASE)
        text = re.sub(r'mit CCW Nicht anwendbar Nicht getestet Nicht.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Vorhanden.*?Nicht\s+Vorhanden', '', text, flags=re.IGNORECASE)
        
        # Zusätzliche Reinigungsmuster
        text = re.sub(r'PCI DSS v[\d.]+\s+SAQ D für Händler,\s+Abschnitt \d+:\s+Fragebogen zur Selbstbewertung\s+\w+\s+\d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI DSS v[\d.]+.*?Selbstbewertungsfragebogen.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Abschnitt\s+\d+\s*:\s*Selbstbewertungsfragebogen.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Abschnitt\s+\d+\s*:\s*Fragebogen zur Selbstbewertung.*?(?=\n)', '', text, flags=re.IGNORECASE)
        
        # Reinige isolierte Seitenverweise
        text = re.sub(r'Seite \d+\s*$', '', text, flags=re.MULTILINE | re.IGNORECASE)
        
        # Ersetze mehrfache Zeilenumbrüche durch einfache
        text = re.sub(r'\n\s*\n', '\n\n', text)
        # Entferne Leerzeichen am Anfang/Ende von Zeilen
        lines = [line.strip() for line in text.splitlines()]
        return "\n".join(lines)

    def is_requirement_number(self, line: str) -> str:
        """Checks if a line starts with a valid requirement number"""
        # More precise pattern for requirement numbers
        pattern = r'^(\d+\.\d+(?:\.\d+)*(?:\.\d+)*)\s+'
        match = re.match(pattern, line.strip())
        if match:
            req_num = match.group(1)
            # Avoid page numbers or version numbers and validate range
            parts = req_num.split('.')
            if len(parts) >= 2:
                main_num = int(parts[0])
                # PCI requirements go from 1 to 12
                if 1 <= main_num <= 12:
                    return req_num
        return ""

    def is_test_line(self, line: str) -> bool:
        """Überprüft ob eine Zeile eine Testzeile ist"""
        line_clean = line.strip()
        # Prüfe exakte Übereinstimmungen zuerst
        if any(line_clean.startswith(indicator) for indicator in self.test_indicators):
            return True
        # Prüfe auf häufige Testmuster - deutsche Verben können am Ende stehen
        test_verbs = ['untersuchen', 'befragen', 'prüfen', 'überprüfen', 'bewerten', 'beobachten', 'kontrollieren', 'inspizieren']
        # Prüfe ob Zeile mit • beginnt UND ein Testverb irgendwo enthält
        if line_clean.startswith('•'):
            for verb in test_verbs:
                if verb in line_clean.lower():
                    return True
        return False

    def extract_requirement_text(self, line: str, req_num: str) -> str:
        """Extracts requirement text by removing the number"""
        # Find position after requirement number
        pattern = rf'^{re.escape(req_num)}\s+'
        cleaned_line = re.sub(pattern, '', line.strip())
        return cleaned_line

    def parse_requirements(self, content: str) -> List[Dict[str, Any]]:
        """Parse requirements from text content"""
        requirements = []
        lines = content.splitlines()
        i = 0
        current_req = None

        while i < len(lines):
            line = lines[i].strip()
            
            if not line:  # Skip empty lines
                i += 1
                continue

            # Check if it's the start of a new requirement
            req_num = self.is_requirement_number(line)
            if req_num:
                # Save previous requirement if it exists
                if current_req:
                    self._finalize_requirement(current_req)
                    if not any(req['req_num'] == current_req['req_num'] for req in requirements):
                        requirements.append(current_req)

                # Initialize new requirement
                req_text = self.extract_requirement_text(line, req_num)
                current_req = {
                    'req_num': req_num,
                    'text': req_text,
                    'tests': [],
                    'guidance': ''
                }
                i += 1
                continue

            # If a requirement is being processed
            if current_req:
                # Check if it's a test line
                if self.is_test_line(line):
                    # Extract complete test preserving action verb
                    test_text = line
                    # Clean bullet but keep verb
                    test_text = re.sub(r'^•\s*', '', test_text).strip()
                    
                    # Gather continuation lines for this test
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue
                        # Stop if we find a new requirement, new test, or special section
                        if (self.is_requirement_number(next_line) or 
                            self.is_test_line(next_line) or
                            next_line.startswith(self.applicability_marker) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break
                        # Add continuation to current test
                        test_text += " " + next_line
                        j += 1
                    
                    # Clean test of artifacts
                    test_text = self._clean_test_text(test_text)
                    if test_text and len(test_text) > 10:  # Only significant tests
                        current_req['tests'].append(test_text)
                    
                    i = j
                    continue

                # Check if it's Applicability Notes section
                elif line.startswith(self.applicability_marker):
                    # Extract applicability notes content in guidance field
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
                    
                    # Clean and store in guidance
                    current_req['guidance'] = self._clean_guidance_text(guidance_text)
                    i = j
                    continue

                # Check if it's Guidance section
                elif line.startswith(self.guidance_marker):
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

                # Check if it's content to ignore
                elif self._should_ignore_line(line):
                    i += 1
                    continue

                # Otherwise, it's text belonging to the main requirement
                else:
                    # Check if text contains hidden tests
                    # AND handle multi-line tests
                    cleaned_line, j = self._extract_tests_from_text_line_multiline(line, current_req, lines, i)
                    
                    # If we processed additional lines for multi-line tests
                    if j > i:
                        i = j
                        continue
                    
                    # Add to main text only if it's not redundant or parasitic
                    if cleaned_line and self._is_valid_text_line(cleaned_line, current_req['text']):
                        if current_req['text']:
                            current_req['text'] += " " + cleaned_line
                        else:
                            current_req['text'] = cleaned_line

            i += 1

        # Save last requirement
        if current_req:
            self._finalize_requirement(current_req)
            if not any(req['req_num'] == current_req['req_num'] for req in requirements):
                requirements.append(current_req)

        return requirements

    def _extract_tests_from_text_line_multiline(self, line: str, current_req: Dict[str, Any], all_lines: List[str], current_index: int) -> Tuple[str, int]:
        """Extracts hidden tests from a text line and handles multi-line tests"""
        remaining_text = line
        processed_lines = current_index
        
        # Find all tests in the line
        for indicator in self.test_indicators:
            verb = indicator[2:]  # Remove "• " to get just "Examine", "Observe", etc.
            pattern = rf'•\s*{re.escape(verb)}[^•]*'
            matches = list(re.finditer(pattern, remaining_text, re.IGNORECASE))
            
            for match in reversed(matches):  # Process right to left to preserve positions
                test_text = match.group(0)
                test_text = re.sub(r'^•\s*', '', test_text).strip()
                
                # Check if test seems incomplete (very short or ends abruptly)
                # and gather following lines if necessary
                if len(test_text) < 30 or not test_text.endswith('.'):
                    # Gather following lines for this test
                    j = current_index + 1
                    while j < len(all_lines):
                        next_line = all_lines[j].strip()
                        if not next_line:
                            j += 1
                            continue
                        
                        # Stop if we find a new requirement, new test, or special section
                        if (self.is_requirement_number(next_line) or 
                            self.is_test_line(next_line) or
                            next_line.startswith(self.applicability_marker) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break
                        
                        # Add continuation to current test
                        test_text += " " + next_line
                        processed_lines = j  # Mark this line as processed
                        
                        # If we have a complete sentence (ends with . ! or ?), we can stop
                        if next_line.endswith('.') or next_line.endswith('!') or next_line.endswith('?'):
                            break
                        
                        j += 1
                
                # Clean test of artifacts
                test_text = self._clean_test_text(test_text)
                
                if test_text and len(test_text) > 10:
                    # Add test if it doesn't already exist
                    if test_text not in current_req['tests']:
                        current_req['tests'].append(test_text)
                    
                    # Remove test from remaining text
                    remaining_text = remaining_text[:match.start()] + ' ' + remaining_text[match.end():]
        
        # Clean remaining text
        remaining_text = re.sub(r'\s+', ' ', remaining_text).strip()
        return remaining_text, processed_lines

    def _extract_tests_from_text_line(self, line: str, current_req: Dict[str, Any]) -> str:
        """Extracts hidden tests from a text line and adds them to tests[] (simple version)"""
        remaining_text = line
        
        # Find all tests in the line
        for indicator in self.test_indicators:
            verb = indicator[2:]  # Remove "• " to get just "Examine", "Observe", etc.
            pattern = rf'•\s*{re.escape(verb)}[^•]*'
            matches = list(re.finditer(pattern, remaining_text, re.IGNORECASE))
            
            for match in reversed(matches):  # Process right to left to preserve positions
                test_text = match.group(0)
                test_text = re.sub(r'^•\s*', '', test_text).strip()
                test_text = self._clean_test_text(test_text)
                
                if test_text and len(test_text) > 10:
                    # Add test if it doesn't already exist
                    if test_text not in current_req['tests']:
                        current_req['tests'].append(test_text)
                    
                    # Remove test from remaining text
                    remaining_text = remaining_text[:match.start()] + ' ' + remaining_text[match.end():]
        
        # Clean remaining text
        remaining_text = re.sub(r'\s+', ' ', remaining_text).strip()
        return remaining_text

    def _clean_test_text(self, text: str) -> str:
        """Cleans test text by removing artifacts"""
        # Remove layout artifacts - more comprehensive patterns
        text = re.sub(r'PCI DSS SAQ D.*?Page \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?All Rights Reserved.*?Page \d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?LLC.*?All Rights Reserved.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI Security Standards Council.*?LLC.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'All Rights Reserved.*?Page \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Page \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'In Place.*?Not in Place', '', text, flags=re.IGNORECASE)
        text = re.sub(r'♦\s*Informationen zu diesen.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'© 2006\s*[−-]\s*\d+\s+PCI DSS\s*[−-]\s*Anforderung\s+Erwartetes\s+Testen\s+Antwort♦', '', text, flags=re.IGNORECASE)
        
        # Remove response table artifacts
        text = re.sub(r'with CCW Not Applicable Not Tested Not.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'In Place\s+In Place with CCW\s+Not Applicable\s+Not Tested\s+Not in Place', '', text, flags=re.IGNORECASE)
        text = re.sub(r'(In Place|Not in Place|Not Applicable|Not Tested|CCW)(\s+(In Place|Not in Place|Not Applicable|Not Tested|CCW))*', '', text, flags=re.IGNORECASE)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _clean_guidance_text(self, text: str) -> str:
        """Cleans guidance text by removing artifacts"""
        # Remove similar artifacts - more comprehensive patterns
        text = re.sub(r'PCI DSS SAQ D.*?Page \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?All Rights Reserved.*?Page \d+', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'© 2006[−-]\d+.*?LLC.*?All Rights Reserved.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'PCI Security Standards Council.*?LLC.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'All Rights Reserved.*?Page \d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Page \d+.*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'In Place.*?Not in Place', '', text, flags=re.IGNORECASE)
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _is_valid_text_line(self, line: str, current_text: str) -> bool:
        """Checks if a line is valid to be added to main text"""
        # Avoid repetitions
        if line in current_text:
            return False
        
        # Avoid lines too short or that seem to be artifacts
        if len(line) < 3:
            return False
            
        # Avoid lines that start with non-test bullets
        if line.startswith('•') and not self.is_test_line('• ' + line[1:]):
            return True
            
        return True

    def _should_ignore_line(self, line: str) -> bool:
        """Determines if a line should be ignored"""
        ignore_patterns = [
            r'^PCI DSS SAQ D',
            r'^© 2006[−-]\d+',
            r'^Page \d+',
            r'^October 2024',
            r'^PCI DSS Requirement',
            r'^Testing Procedures',
            r'^Response',
            r'^In Place',
            r'^Not in Place',
            r'^Not Applicable',
            r'^Not Tested',
            r'^♦ Informationen',
            r'^© 2006\s*[−-]\s*\d+\s+PCI DSS\s*[−-]\s*Anforderung',
            r'^\(Check one response',
            r'^Section \d+',
            r'^All Rights Reserved',
            r'^LLC\.',
            r'^PCI Security Standards Council',
            r'^Self\s*-\s*Assessment\s+Questionnaire',
            r'^PCI DSS v[\d.]+',
            r'© 2006[−-]\d+.*PCI Security Standards Council.*LLC.*All Rights Reserved.*Page \d+',
        ]
        
        line_lower = line.lower()
        for pattern in ignore_patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return True
                
        # Check if line contains copyright artifacts anywhere
        if re.search(r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?All Rights Reserved', line, re.IGNORECASE):
            return True
            
        # Ignore very short lines that are probably noise
        if len(line.strip()) <= 2:
            return True
            
        return False

    def _remove_response_artifacts(self, text: str) -> str:
        """Removes questionnaire response checkbox artifacts"""
        # Remove all variations of response checkboxes and copyright/page artifacts
        patterns_to_remove = [
            r'with CCW Not Applicable Not Tested Not.*?(?=\n|$)',
            r'In Place\s+In Place with CCW\s+Not Applicable\s+Not Tested\s+Not in Place',
            r'with CCW\s+Not Applicable\s+Not Tested\s+Not in Place',
            r'In Place.*?Not in Place.*?(?=\n|$)',
            r'(In Place|Not in Place|Not Applicable|Not Tested|CCW)(\s+(In Place|Not in Place|Not Applicable|Not Tested|CCW))+',
            r'♦\s*Informationen zu diesen.*?(?=\n|$)',
            r'© 2006\s*[−-]\s*\d+\s+PCI DSS\s*[−-]\s*Anforderung\s+Erwartetes\s+Testen\s+Antwort♦.*?(?=\n|$)',
            r'\(Check one response.*?\)',
            r'© 2006[−-]\d+.*?PCI Security Standards Council.*?LLC.*?All Rights Reserved.*?Page \d+',
            r'© 2006[−-]\d+.*?LLC.*?All Rights Reserved.*',
            r'PCI Security Standards Council.*?LLC.*',
            r'All Rights Reserved.*?Page \d+',
            r'Page \d+[^\w]*$',
        ]
        
        for pattern in patterns_to_remove:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Normalize spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _finalize_requirement(self, req: Dict[str, Any]):
        """Cleans and finalizes a requirement before saving"""
        # Extract remaining tests from main text
        text_remaining = self._extract_tests_from_text_line(req['text'], req)
        req['text'] = text_remaining
        
        # Remove response checkbox artifacts from main text
        req['text'] = self._remove_response_artifacts(req['text'])
        
        # Clean main text
        req['text'] = req['text'].strip()
        req['text'] = re.sub(r'\s+', ' ', req['text'])  # Normalize spaces
        
        # Clean tests and remove duplicates
        cleaned_tests = []
        for test in req['tests']:
            test_clean = self._remove_response_artifacts(test)
            test_clean = test_clean.strip()
            test_clean = re.sub(r'\s+', ' ', test_clean)
            if test_clean and test_clean not in cleaned_tests and len(test_clean) > 10:
                cleaned_tests.append(test_clean)
        req['tests'] = cleaned_tests
        
        # Clean guidance
        req['guidance'] = self._remove_response_artifacts(req['guidance'])
        req['guidance'] = req['guidance'].strip()
        req['guidance'] = re.sub(r'\s+', ' ', req['guidance'])

    def extract_all_requirements(self) -> List[Dict[str, Any]]:
        """Extracts all requirements from PDF"""
        print("Reading PDF...")
        raw_text = self.read_pdf_content()
        if not raw_text:
            print("Failed to read PDF.")
            return []

        print("Cleaning text...")
        clean_text = self.clean_text(raw_text)

        print("Parsing requirements...")
        self.requirements = self.parse_requirements(clean_text)
        return self.requirements

    def print_summary(self):
        """Displays a summary of extracted requirements"""
        print(f"\nExtraction Summary:")
        print(f"Total requirements extracted: {len(self.requirements)}")
        if self.requirements:
            print(f"First requirement: {self.requirements[0]['req_num']}")
            print(f"Last requirement: {self.requirements[-1]['req_num']}")
            
            # Statistics
            with_tests = sum(1 for req in self.requirements if req['tests'])
            with_guidance = sum(1 for req in self.requirements if req['guidance'])
            total_tests = sum(len(req['tests']) for req in self.requirements)
            
            print(f"Requirements with tests: {with_tests}")
            print(f"Requirements with guidance: {with_guidance}")
            print(f"Total tests extracted: {total_tests}")

    def save_to_json(self, output_file: str = "pci_requirements_v5_EN.json"):
        """Saves requirements to JSON format"""
        # Sort by requirement number
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
        print(f"Requirements saved to {output_file}")

    def save_to_csv(self, output_file: str = "pci_requirements_v5_EN.csv"):
        """Saves requirements to CSV format with simplified structure"""
        import csv
        
        # Sort by requirement number
        def sort_key(req):
            parts = [int(x) for x in req['req_num'].split('.')]
            while len(parts) < 4:
                parts.append(0)
            return parts
        
        sorted_requirements = sorted(self.requirements, key=sort_key)
        
        # Prepare data for CSV with simplified structure
        csv_data = []
        for req in sorted_requirements:
            # Convert tests to semicolon-separated string
            tests_str = " ; ".join(req['tests']) if req['tests'] else ""
            
            csv_row = {
                'req_num': req['req_num'],
                'text': req['text'],
                'tests': tests_str,
                'guidance': req['guidance']
            }
            csv_data.append(csv_row)
        
        # Save to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['req_num', 'text', 'tests', 'guidance']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(csv_data)
        
        print("=" * 60)
        print(f"Requirements saved to {output_file}")
        print(f"CSV structure: {len(csv_data)} requirements with simplified columns")

def main():
    pdf_path = "/Users/thomasmionnet/Downloads/PCI-DSS-v4-0-1-SAQ-D-Merchant-DE.pdf"
    print("PCI DSS DEUTSCHER EXTRAKTOR")
    print("=" * 60)
    
    extractor = PCIRequirementsExtractor(pdf_path)
    requirements = extractor.extract_all_requirements()

    if requirements:
        extractor.print_summary()
        extractor.save_to_csv("pci_requirements_v5_DE.csv")
        extractor.save_to_json("pci_requirements_v5_DE.json")  # Behalte auch JSON für Kompatibilität
    else:
        print("No requirements could be extracted.")

if __name__ == "__main__":
    main()