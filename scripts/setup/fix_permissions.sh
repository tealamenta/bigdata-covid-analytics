#!/bin/bash
# ============================================================================
# SCRIPT DE CONFIGURATION DES PERMISSIONS
# ============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo " Configuration des permissions du projet..."
echo " Répertoire: $PROJECT_ROOT"
echo ""

# Aller dans le répertoire du projet
cd "$PROJECT_ROOT"

# 1. Dossiers : 755 (rwxr-xr-x)
echo " Configuration des dossiers (755)..."
find . -type d -exec chmod 755 {} \;

# 2. Fichiers normaux : 644 (rw-r--r--)
echo " Configuration des fichiers (644)..."
find . -type f -exec chmod 644 {} \;

# 3. Scripts Python : 755 (exécutables)
echo " Scripts Python exécutables..."
find src/ -name "*.py" -exec chmod 755 {} \;
find tests/ -name "*.py" -exec chmod 755 {} \;

# 4. Scripts shell : 755 (exécutables)
echo " Scripts shell exécutables..."
find scripts/ -name "*.sh" -exec chmod 755 {} \; 2>/dev/null || true

# 5. Protéger les fichiers sensibles (si existants)
echo " Protection fichiers sensibles..."
[ -f .env ] && chmod 600 .env
[ -f config/secrets.env ] && chmod 600 config/secrets.env

# 6. Logs et data : accessibles en écriture
echo " Permissions logs et data..."
chmod -R 755 logs/ 2>/dev/null || true
chmod -R 755 data/ 2>/dev/null || true
chmod -R 755 results/ 2>/dev/null || true
chmod -R 755 metrics/ 2>/dev/null || true

# 7. Vérifier Docker (si lancé en root)
echo " Vérification permissions Docker..."
if [ -d "airflow/logs" ]; then
    chmod -R 777 airflow/logs/ 2>/dev/null || true
fi

echo ""
echo " Permissions configurées avec succès!"
echo ""
echo " Résumé:"
echo "  - Dossiers: 755 (rwxr-xr-x)"
echo "  - Fichiers: 644 (rw-r--r--)"
echo "  - Scripts Python: 755 (exécutables)"
echo "  - Scripts shell: 755 (exécutables)"
echo "  - Fichiers sensibles: 600 (privés)"
echo ""
echo " Tu peux maintenant lancer: make up"
