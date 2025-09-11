#!/bin/bash

# Script de lancement pour PCI Auto Scraper
# Utilise l'environnement virtuel automatiquement

echo "🚀 Lancement du PCI Auto Scraper"
echo "================================="

# Obtenir le répertoire du script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Vérifier que l'environnement virtuel existe
if [ ! -f "venv/bin/python" ]; then
    echo "❌ Environnement virtuel non trouvé"
    echo "🔧 Créez l'environnement virtuel avec:"
    echo "   python3 -m venv venv"
    echo "   source venv/bin/activate"
    echo "   pip install -r requirements.txt"
    exit 1
fi

# Lancer le script avec l'environnement virtuel
echo "🔧 Utilisation de l'environnement virtuel: $(pwd)/venv"
echo ""

./venv/bin/python pci_auto_scraper.py

echo ""
echo "✅ Exécution terminée"
