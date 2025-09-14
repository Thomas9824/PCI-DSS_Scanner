#!/usr/bin/env python3
"""
Extracteur intelligent d'exigences PCI DSS - Version française spécialisée (V5)
Module d'extraction et parsing avancé pour documents SAQ/PCI DSS français
Architecture : PDF Reading -> Text Cleaning -> Pattern Recognition -> Structured Extraction
Format structuré: {'req_num': '...', 'text': '...', 'tests': [...], 'guidance': '...'}
"""
import re  # Expressions régulières pour parsing et nettoyage de texte
import json  # Export des données structurées en format JSON
import PyPDF2  # Extraction de texte depuis fichiers PDF
from typing import List, Dict, Any, Tuple  # Annotations de types pour robustesse du code

class PCIRequirementsExtractor:
    """
    Extracteur spécialisé pour documents PCI DSS français avec parsing intelligent
    Combine détection automatique de structure, nettoyage de texte et extraction multi-patterns
    """

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path  # Chemin vers le fichier PDF source
        self.requirements = []    # Cache des exigences extraites avec métadonnées

        # Patterns de reconnaissance spécifiques au français pour les procédures de test
        self.test_indicators = ['• Examiner', '• Observer', '• Interroger', '• Vérifier', '• Inspecter']

        # Marqueurs de sections spécialisées (terminologie française officielle PCI DSS)
        self.applicability_marker = "Notes d'Applicabilité"  # Section notes d'applicabilité
        self.guidance_marker = "Conseils"                   # Section conseils/guidance

    def find_start_page(self) -> int:
        """Détecteur automatique de page de début par recherche pattern 1.1.1"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)

                # Balayage séquentiel pour localiser la première exigence PCI DSS (1.1.1)
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()

                    # Pattern matching : recherche de "1.1.1" en début de ligne
                    if re.search(r'^1\.1\.1\s+', page_text, re.MULTILINE):
                        print(f"Page de début détectée: {page_num + 1} (contient 1.1.1)")
                        return page_num

        except Exception as e:
            print(f"Erreur lors de la recherche de la page de début: {e}")

        # Stratégie de fallback avec page standard
        print("Page de début non trouvée, utilisation de la page 16 par défaut")
        return 15
    
    def find_end_page(self) -> int:
        """Algorithme de détection de fin basé sur la hiérarchie des exigences PCI DSS"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                highest_requirement = ""
                end_page = len(pdf_reader.pages) - 1

                # Pattern pour numérotation hiérarchique PCI DSS (ex: 12.3.4.1)
                pattern = r'^(\d+\.\d+(?:\.\d+)*(?:\.\d+)*)\s+'

                # Balayage complet pour identifier la dernière exigence valide
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()
                    matches = re.findall(pattern, page_text, re.MULTILINE)

                    for match in matches:
                        # Validation : exigences PCI DSS dans la plage 1-12
                        parts = match.split('.')
                        if len(parts) >= 2:
                            main_num = int(parts[0])
                            if 1 <= main_num <= 12:
                                # Comparaison hiérarchique pour trouver la plus haute exigence
                                if self._is_higher_requirement(match, highest_requirement):
                                    highest_requirement = match
                                    end_page = page_num

                if highest_requirement:
                    print(f"Page de fin détectée: {end_page + 1} (dernière exigence: {highest_requirement})")
                    return end_page

        except Exception as e:
            print(f"Erreur lors de la recherche de la page de fin: {e}")

        # Fallback : page standard pour documents SAQ
        print("Page de fin non trouvée, utilisation de la page 129 par défaut")
        return 128
    
    def _is_higher_requirement(self, req1: str, req2: str) -> bool:
        """Comparateur hiérarchique pour numérotation PCI DSS (ex: 12.3.4 > 12.3.3)"""
        if not req2:
            return True

        # Conversion en arrays numériques pour comparaison lexicographique
        parts1 = [int(x) for x in req1.split('.')]
        parts2 = [int(x) for x in req2.split('.')]

        # Normalisation des longueurs avec padding de zéros
        max_len = max(len(parts1), len(parts2))
        parts1.extend([0] * (max_len - len(parts1)))
        parts2.extend([0] * (max_len - len(parts2)))

        # Comparaison lexicographique des composants hiérarchiques
        return parts1 > parts2

    def read_pdf_content(self) -> str:
        """Lecteur PDF intelligent avec détection automatique des boundaries d'extraction"""
        try:
            with open(self.pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""

                # Phase 1: Détection automatique des limites du document
                start_page = self.find_start_page()
                end_page = self.find_end_page()

                # Validation et correction des boundaries
                if end_page <= start_page:
                    end_page = min(len(pdf_reader.pages) - 1, start_page + 100)  # Limite sécurisée
                    print(f"Ajustement de la page de fin à {end_page + 1}")

                print(f"Extraction des pages {start_page + 1} à {end_page + 1}")

                # Phase 2: Extraction séquentielle du texte brut
                for page_num in range(start_page, end_page + 1):
                    if page_num < len(pdf_reader.pages):
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text() + "\n"

            return text
        except Exception as e:
            print(f"Erreur lors de la lecture du PDF: {e}")
            return ""

    def clean_text(self, text: str) -> str:
        """Système de nettoyage avancé pour suppression des artefacts SAQ français"""
        # Suppression des headers/footers récurrents du document SAQ
        text = re.sub(r'SAQ D de PCI DSS v[\d.]+.*?Page \d+.*?(?:En Place|Pas en Place)', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Nettoyage des métadonnées de copyright et version
        text = re.sub(r'© 2006-\d+.*?LLC.*?Tous Droits Réservés\.', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Octobre 2024', '', text, flags=re.IGNORECASE)

        # Suppression des références croisées et instructions de navigation
        text = re.sub(r'♦\s*Se reporter.*?(?=\n)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\(Cocher une réponse.*?\)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Section \d+ :', '', text, flags=re.IGNORECASE)

        # Nettoyage spécialisé des tableaux de conformité SAQ
        text = re.sub(r'En Place\s+En Place avec CCW\s+Non Applicable\s+Non Testé\s+Pas en Place', '', text, flags=re.IGNORECASE)
        text = re.sub(r'avec CCW\s+Non Applicable\s+Non Testé\s+Pas en Place', '', text, flags=re.IGNORECASE)
        text = re.sub(r'avec CCW Non Applicable Non Testé Pas.*', '', text, flags=re.IGNORECASE)

        # Normalisation des espaces et mise en forme
        text = re.sub(r'\n\s*\n', '\n\n', text)  # Consolidation des sauts de ligne
        lines = [line.strip() for line in text.splitlines()]  # Trim de chaque ligne
        return "\n".join(lines)

    def is_requirement_number(self, line: str) -> str:
        """Validateur de numérotation hiérarchique PCI DSS avec validation de plage"""
        # Pattern regex pour structure hiérarchique (ex: 1.2.3.4)
        pattern = r'^(\d+\.\d+(?:\.\d+)*(?:\.\d+)*)\s+'
        match = re.match(pattern, line.strip())
        if match:
            req_num = match.group(1)

            # Validation de conformité PCI DSS : plage 1-12 pour exigences principales
            parts = req_num.split('.')
            if len(parts) >= 2:
                main_num = int(parts[0])
                if 1 <= main_num <= 12:  # Scope PCI DSS officiel
                    return req_num
        return ""

    def is_test_line(self, line: str) -> bool:
        """Détecteur de procédures de test basé sur les verbes d'action français"""
        line_clean = line.strip()
        # Matching des indicateurs de test français PCI DSS
        return any(line_clean.startswith(indicator) for indicator in self.test_indicators)

    def extract_requirement_text(self, line: str, req_num: str) -> str:
        """Extracteur de texte d'exigence avec suppression du préfixe numérique"""
        # Pattern matching et suppression du numéro d'exigence + espaces
        pattern = rf'^{re.escape(req_num)}\s+'
        cleaned_line = re.sub(pattern, '', line.strip())
        return cleaned_line

    def parse_requirements(self, content: str) -> List[Dict[str, Any]]:
        """Parser principal avec machine à états pour extraction structurée des exigences PCI DSS"""
        requirements = []
        lines = content.splitlines()
        i = 0
        current_req = None

        # Machine à états : ligne par ligne avec gestion contextuelle
        while i < len(lines):
            line = lines[i].strip()

            if not line:  # Skip des lignes vides
                i += 1
                continue

            # État 1: Détection d'une nouvelle exigence
            req_num = self.is_requirement_number(line)
            if req_num:
                # Finalisation et sauvegarde de l'exigence précédente
                if current_req:
                    self._finalize_requirement(current_req)
                    if not any(req['req_num'] == current_req['req_num'] for req in requirements):
                        requirements.append(current_req)

                # Initialisation d'une nouvelle exigence avec structure complète
                req_text = self.extract_requirement_text(line, req_num)
                current_req = {
                    'req_num': req_num,        # Numéro hiérarchique (ex: 1.2.3)
                    'text': req_text,          # Texte principal de l'exigence
                    'tests': [],               # Array des procédures de test
                    'guidance': ''             # Notes d'applicabilité et conseils
                }
                i += 1
                continue

            # État 2: Traitement contextuel selon le type de contenu
            if current_req:
                # État 2a: Détection et extraction de procédure de test
                if self.is_test_line(line):
                    # Préservation du verbe d'action français (Examiner, Observer, etc.)
                    test_text = line
                    test_text = re.sub(r'^•\s*', '', test_text).strip()  # Suppression puce

                    # Agrégation multi-lignes pour tests complexes
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue

                        # Conditions d'arrêt : nouvelle section détectée
                        if (self.is_requirement_number(next_line) or
                            self.is_test_line(next_line) or
                            next_line.startswith(self.applicability_marker) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break

                        # Continuation du test en cours
                        test_text += " " + next_line
                        j += 1

                    # Nettoyage et validation avant stockage
                    test_text = self._clean_test_text(test_text)
                    if test_text and len(test_text) > 10:  # Filtre des tests significatifs
                        current_req['tests'].append(test_text)

                    i = j
                    continue

                # État 2b: Extraction des Notes d'Applicabilité
                elif line.startswith(self.applicability_marker):
                    # Extraction du contenu guidance avec agrégation multi-lignes
                    guidance_text = line[len(self.applicability_marker):].strip(': ')
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue

                        # Arrêt si nouvelle section détectée
                        if (self.is_requirement_number(next_line) or
                            self.is_test_line(next_line) or
                            next_line.startswith(self.guidance_marker) or
                            self._should_ignore_line(next_line)):
                            break

                        guidance_text += " " + next_line
                        j += 1

                    # Nettoyage et stockage dans le champ guidance
                    current_req['guidance'] = self._clean_guidance_text(guidance_text)
                    i = j
                    continue

                # État 2c: Extraction de la section Conseils
                elif line.startswith(self.guidance_marker):
                    guidance_text = line[len(self.guidance_marker):].strip(': ')
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if not next_line:
                            j += 1
                            continue

                        # Conditions d'arrêt similaires aux autres sections
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

                # État 2d: Filtrage du contenu non pertinent
                elif self._should_ignore_line(line):
                    i += 1
                    continue

                # État 2e: Traitement du texte principal de l'exigence
                else:
                    # Extraction des tests intégrés dans le texte + gestion multi-lignes
                    cleaned_line, j = self._extract_tests_from_text_line_multiline(line, current_req, lines, i)

                    # Avancement de l'index si des lignes additionnelles ont été traitées
                    if j > i:
                        i = j
                        continue

                    # Agrégation au texte principal avec validation anti-redondance
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
        """Extrait les tests cachés dans une ligne de texte et gère les tests multi-lignes

        Méthode avancée de détection et extraction des procédures de test intégrées
        dans le texte principal avec support de la continuité multi-lignes
        """
        remaining_text = line
        processed_lines = current_index

        # Scanner tous les indicateurs de test pour détection exhaustive
        for indicator in self.test_indicators:
            verb = indicator[2:]  # Extraction du verbe d'action français ("Examiner", "Observer", etc.)
            pattern = rf'•\s*{re.escape(verb)}[^•]*'  # Pattern regex pour détecter le test avec puce
            matches = list(re.finditer(pattern, remaining_text, re.IGNORECASE))  # Recherche insensible à la casse
            
            for match in reversed(matches):  # Traitement inverse pour préserver les positions des regex
                test_text = match.group(0)
                test_text = re.sub(r'^•\s*', '', test_text).strip()  # Suppression de la puce initiale

                # Détection des tests incomplets nécessitant une agrégation multi-lignes
                # Critères: longueur insuffisante ou absence de ponctuation finale
                if len(test_text) < 30 or not test_text.endswith('.'):
                    # Algorithme d'agrégation séquentielle pour tests multi-lignes
                    j = current_index + 1
                    while j < len(all_lines):  # Parcours des lignes suivantes
                        next_line = all_lines[j].strip()
                        if not next_line:
                            j += 1
                            continue

                        # Conditions d'arrêt : détection de nouvelles sections structurelles
                        if (self.is_requirement_number(next_line) or   # Nouvelle exigence détectée
                            self.is_test_line(next_line) or            # Nouveau test indépendant
                            next_line.startswith(self.applicability_marker) or  # Section applicabilité
                            next_line.startswith(self.guidance_marker) or       # Section conseils
                            self._should_ignore_line(next_line)):       # Contenu non pertinent
                            break

                        # Agrégation de la continuation avec espace séparateur
                        test_text += " " + next_line
                        processed_lines = j  # Marquage de ligne comme traitée pour éviter la redondance

                        # Détection de fin de phrase complète pour optimiser l'arrêt
                        if next_line.endswith('.') or next_line.endswith('!') or next_line.endswith('?'):
                            break  # Arrêt sur ponctuation terminale

                        j += 1
                
                # Pipeline de nettoyage pour suppression des artefacts PDF
                test_text = self._clean_test_text(test_text)

                if test_text and len(test_text) > 10:  # Filtre de qualité minimum
                    # Déduplication automatique des tests identiques
                    if test_text not in current_req['tests']:
                        current_req['tests'].append(test_text)

                    # Suppression du segment test du texte principal (chirurgie de string)
                    remaining_text = remaining_text[:match.start()] + ' ' + remaining_text[match.end():]
        
        # Normalisation finale : compression des espaces multiples
        remaining_text = re.sub(r'\s+', ' ', remaining_text).strip()
        return remaining_text, processed_lines  # Retourne le texte nettoyé + index de fin

    def _extract_tests_from_text_line(self, line: str, current_req: Dict[str, Any]) -> str:
        """Extrait les tests cachés dans une ligne de texte et les ajoute à tests[] (version simple)

        Version simplifiée sans gestion multi-lignes pour les tests complets sur une ligne
        """
        remaining_text = line

        # Détection exhaustive des indicateurs de test dans la ligne
        for indicator in self.test_indicators:
            verb = indicator[2:]  # Extraction du verbe d'action français seul
            pattern = rf'•\s*{re.escape(verb)}[^•]*'  # Pattern d'identification test avec puce
            matches = list(re.finditer(pattern, remaining_text, re.IGNORECASE))  # Recherche insensible casse
            
            for match in reversed(matches):  # Traitement inverse pour conservation des positions
                test_text = match.group(0)
                test_text = re.sub(r'^•\s*', '', test_text).strip()  # Suppression puce initiale
                test_text = self._clean_test_text(test_text)  # Pipeline de nettoyage artefacts

                if test_text and len(test_text) > 10:  # Filtre qualité longueur minimum
                    # Système de déduplication pour éviter les tests redondants
                    if test_text not in current_req['tests']:
                        current_req['tests'].append(test_text)

                    # Extraction chirurgicale du segment test du texte principal
                    remaining_text = remaining_text[:match.start()] + ' ' + remaining_text[match.end():]
        
        # Normalisation finale avec compression des espaces
        remaining_text = re.sub(r'\s+', ' ', remaining_text).strip()
        return remaining_text  # Retour du texte nettoyé sans les tests extraits

    def _clean_test_text(self, text: str) -> str:
        """Nettoie le texte d'un test en supprimant les artefacts

        Pipeline de nettoyage spcialisé pour supprimer les éléments parasites
        liés à la conversion PDF et aux tableaux de réponse
        """
        # Suppression des artefacts de mise en page PDF
        text = re.sub(r'SAQ D de PCI DSS.*?Page \d+.*', '', text, flags=re.IGNORECASE)  # Headers/footers
        text = re.sub(r'© 2006-.*?LLC.*', '', text, flags=re.IGNORECASE)  # Copyright notices
        text = re.sub(r'En Place.*?Pas en Place', '', text, flags=re.IGNORECASE)  # Status indicators
        text = re.sub(r'♦\s*Se reporter.*', '', text, flags=re.IGNORECASE)  # Cross-references
        
        # Élimination des artefacts de tableaux de réponse de conformité
        text = re.sub(r'avec CCW Non Applicable Non Testé Pas.*', '', text, flags=re.IGNORECASE)  # Fragments de tableau
        text = re.sub(r'En Place\s+En Place avec CCW\s+Non Applicable\s+Non Testé\s+Pas en Place', '', text, flags=re.IGNORECASE)  # Headers complets
        text = re.sub(r'(En Place|Pas en Place|Non Applicable|Non Testé|CCW)(\s+(En Place|Pas en Place|Non Applicable|Non Testé|CCW))*', '', text, flags=re.IGNORECASE)  # Séquences de status

        # Normalisation finale des espaces et retour
        text = re.sub(r'\s+', ' ', text)  # Compression des espaces multiples
        return text.strip()  # Suppression espaces de début/fin

    def _clean_guidance_text(self, text: str) -> str:
        """Nettoie le texte de guidance en supprimant les artefacts

        Nettoyage spécialisé pour les sections de conseils et notes d'applicabilité
        """
        # Application du même pipeline de nettoyage que les tests
        text = re.sub(r'SAQ D de PCI DSS.*?Page \d+.*', '', text, flags=re.IGNORECASE)  # Headers PDF
        text = re.sub(r'© 2006-.*?LLC.*', '', text, flags=re.IGNORECASE)  # Copyrights
        text = re.sub(r'En Place.*?Pas en Place', '', text, flags=re.IGNORECASE)  # Status artifacts
        # Normalisation et retour du texte guidance nettoyé
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _is_valid_text_line(self, line: str, current_text: str) -> bool:
        """Vérifie si une ligne est valide pour être ajoutée au texte principal

        Algorithme de validation pour éviter les redondances et artefacts
        """
        # Détection anti-redondance : éviter les répétitions exactes
        if line in current_text:
            return False

        # Filtre qualité : éliminer les lignes trop courtes (probablement des artefacts)
        if len(line) < 3:
            return False

        # Gestion spéciale des puces : accepter les puces non-test valides
        if line.startswith('•') and not self.is_test_line('• ' + line[1:]):
            return True

        return True  # Par défaut, accepter la ligne

    def _should_ignore_line(self, line: str) -> bool:
        """Détermine si une ligne doit être ignorée

        Filtre exhaustif des contenus non pertinents (headers, footers, artefacts)
        """
        # Définition des patterns regex pour filtrage automatique
        ignore_patterns = [
            r'^SAQ D de PCI DSS',          # Headers de document
            r'^© 2006-\d+',               # Copyrights avec années
            r'^Page \d+',                  # Numéros de page
            r'^Octobre 2024',              # Dates de publication
            r'^Exigence de PCI DSS',       # Labels sections
            r'^Tests Prévus',             # Headers de test
            r'^Réponse',                   # Headers de réponse
            r'^En Place',                  # Statuts de conformité
            r'^Pas en Place',
            r'^Non Applicable',
            r'^Non Testé',
            r'^♦ Se reporter',           # Cross-références
            r'^\(Cocher une réponse',      # Instructions UI
            r'^Section \d+',               # Numérotation sections
            r'^Tous Droits Réservés',     # Mentions légales
            r'^LLC\.',                     # Suffixes entreprise
            r'^PCI Security Standards Council',  # Nom organisation
        ]
        
        line_lower = line.lower()  # Conversion en minuscules pour optimisation
        # Balayage de tous les patterns d'exclusion
        for pattern in ignore_patterns:
            if re.match(pattern, line, re.IGNORECASE):  # Match insensible à la casse
                return True

        # Filtre de longueur : éliminer les lignes parasites trop courtes
        if len(line.strip()) <= 2:
            return True

        return False  # Par défaut, ne pas ignorer la ligne

    def _remove_response_artifacts(self, text: str) -> str:
        """Supprime les artefacts de cases de réponse du questionnaire

        Pipeline avanci de suppression des éléments UI et interactifs du formulaire
        """
        # Définition des patterns d'élimination des éléments interactifs
        patterns_to_remove = [
            r'avec CCW Non Applicable Non Testé Pas.*?(?=\n|$)',  # Fragments de tableau de statut
            r'En Place\s+En Place avec CCW\s+Non Applicable\s+Non Testé\s+Pas en Place',  # Header complet tableau
            r'avec CCW\s+Non Applicable\s+Non Testé\s+Pas en Place',  # Header partiel tableau
            r'En Place.*?Pas en Place.*?(?=\n|$)',  # Range de statuts
            r'(En Place|Pas en Place|Non Applicable|Non Testé|CCW)(\s+(En Place|Pas en Place|Non Applicable|Non Testé|CCW))+',  # Séquences multiples
            r'♦\s*Se reporter.*?(?=\n|$)',  # Cross-références avec symboles
            r'\(Cocher une réponse.*?\)',  # Instructions utilisateur
        ]
        
        # Application itérative de tous les patterns de suppression
        for pattern in patterns_to_remove:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)  # Suppression insensible casse

        # Normalisation finale des espaces et retour du texte nettoyé
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _finalize_requirement(self, req: Dict[str, Any]):
        """Nettoie et finalise une exigence avant de la sauvegarder

        Pipeline final de nettoyage et validation avant persistance
        """
        # Extraction finale des tests résiduels dans le texte principal
        text_remaining = self._extract_tests_from_text_line(req['text'], req)
        req['text'] = text_remaining

        # Élimination des artefacts UI du texte principal
        req['text'] = self._remove_response_artifacts(req['text'])

        # Normalisation finale du texte principal
        req['text'] = req['text'].strip()
        req['text'] = re.sub(r'\s+', ' ', req['text'])  # Compression espaces
        
        # Pipeline de nettoyage et déduplication des procédures de test
        cleaned_tests = []
        for test in req['tests']:
            test_clean = self._remove_response_artifacts(test)  # Suppression artefacts
            test_clean = test_clean.strip()
            test_clean = re.sub(r'\s+', ' ', test_clean)  # Normalisation espaces
            # Filtrage qualité : longueur minimum + déduplication
            if test_clean and test_clean not in cleaned_tests and len(test_clean) > 10:
                cleaned_tests.append(test_clean)
        req['tests'] = cleaned_tests
        
        # Nettoyage final de la section guidance/conseils
        req['guidance'] = self._remove_response_artifacts(req['guidance'])  # Suppression artefacts
        req['guidance'] = req['guidance'].strip()
        req['guidance'] = re.sub(r'\s+', ' ', req['guidance'])  # Normalisation espaces

    def extract_all_requirements(self) -> List[Dict[str, Any]]:
        """Extrait toutes les exigences du PDF

        Méthode principale orchestrant tout le pipeline d'extraction
        """
        print("Lecture du PDF...")  # Phase 1: Extraction PDF
        raw_text = self.read_pdf_content()
        if not raw_text:
            print("Échec de la lecture du PDF.")
            return []  # Retour vide en cas d'échec

        print("Nettoyage du texte...")  # Phase 2: Pré-processing
        clean_text = self.clean_text(raw_text)

        print("Parsing des exigences...")  # Phase 3: Parsing structuré
        self.requirements = self.parse_requirements(clean_text)
        return self.requirements  # Retour des exigences extraites

    def print_summary(self):
        """Affiche un résumé des exigences extraites

        Génère des statistiques détaillées sur le processus d'extraction
        """
        print(f"\nRésumé de l'extraction:")  # Header du rapport
        print(f"Nombre total d'exigences extraites: {len(self.requirements)}")
        if self.requirements:
            print(f"Première exigence: {self.requirements[0]['req_num']}")
            print(f"Dernière exigence: {self.requirements[-1]['req_num']}")

            # Calculs statistiques avancés
            with_tests = sum(1 for req in self.requirements if req['tests'])  # Exigences avec tests
            with_guidance = sum(1 for req in self.requirements if req['guidance'])  # Exigences avec conseils
            total_tests = sum(len(req['tests']) for req in self.requirements)  # Nombre total de tests

            # Affichage des métriques de qualité
            print(f"Exigences avec tests: {with_tests}")
            print(f"Exigences avec guidance: {with_guidance}")
            print(f"Total des tests extraits: {total_tests}")

    def save_to_json(self, output_file: str = "pci_requirements_v5.json"):
        """Sauvegarde les exigences au format JSON

        Export structuré avec tri hiérarchique des exigences
        """
        # Fonction de tri hiérarchique par numérotation d'exigence
        def sort_key(req):
            parts = [int(x) for x in req['req_num'].split('.')]  # Découpage numérique
            # Padding avec zéros pour tri cohérent (ex: 1.1 vs 1.10)
            while len(parts) < 4:
                parts.append(0)  # Complétion avec zéros
            return parts
        
        sorted_requirements = sorted(self.requirements, key=sort_key)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sorted_requirements, f, ensure_ascii=False, indent=2)
        print("=" * 60)
        print(f"Exigences sauvegardées dans {output_file}")

    def save_to_csv(self, output_file: str = "pci_requirements_v5.csv"):
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
    pdf_path = "PCI-DSS-v4-0-1-SAQ-D-Merchant-FR.pdf"
    print("EXTRACTEUR PCI DSS FRANÇAIS")
    print("=" * 60)
    
    extractor = PCIRequirementsExtractor(pdf_path)
    requirements = extractor.extract_all_requirements()

    if requirements:
        extractor.print_summary()
        extractor.save_to_csv("pci_requirements_v5.csv")
        extractor.save_to_json("pci_requirements_v5.json")  # Garde aussi JSON pour compatibilité
    else:
        print("Aucune exigence n'a pu être extraite.")

if __name__ == "__main__":
    main()