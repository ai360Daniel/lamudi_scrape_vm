#!/bin/bash

# ====================================================================
# ⚡ SCRIPT RÁPIDO - SOLO DESCARGA Y EJECUTA
# ====================================================================
# Para VMs que YA TIENEN las dependencias instaladas
# ====================================================================

echo "🚀 Iniciando descarga y ejecución..."
echo ""

# Variables
REPO_URL="https://github.com/ai360Daniel/lamudi_scrape_vm.git"
WORK_DIR="$HOME/lamudi_scrape"
SCRIPT_TYPE="${1:-bj_cu}"  # Parámetro: bj_cu o cdmx

# Crear directorio
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# ====================================================================
# Descargar o actualizar repo
# ====================================================================
if [ -d ".git" ]; then
    echo "📥 Actualizando repositorio..."
    git pull origin main
else
    echo "🆕 Clonando repositorio..."
    git clone "$REPO_URL" .
fi

# ====================================================================
# Activar entorno virtual
# ====================================================================
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "⚠️  Entorno virtual no encontrado"
    echo "   Ejecuta primero: python3 -m venv venv"
    exit 1
fi

# ====================================================================
# Ejecutar scraper según tipo
# ====================================================================
case "$SCRIPT_TYPE" in
    bj_cu)
        echo ""
        echo "▶️  Ejecutando: Benito Juárez + Cuauhtémoc-2"
        nohup python -u vm/lamudi_scraper_bj_cu.py > app_bj_cu.log 2>&1 &
        echo "✅ PID: $!"
        echo "📊 Logs: tail -f app_bj_cu.log"
        ;;
    cdmx)
        echo ""
        echo "▶️  Ejecutando: CDMX (todas las alcaldías)"
        nohup python -u vm/lamudi_scraper_cdmx.py > app_cdmx.log 2>&1 &
        echo "✅ PID: $!"
        echo "📊 Logs: tail -f app_cdmx.log"
        ;;
    *)
        echo "❌ Tipo desconocido: $SCRIPT_TYPE"
        echo "   Uso: $0 [bj_cu|cdmx]"
        exit 1
        ;;
esac

echo ""
echo "═══════════════════════════════════════════════════════════════"
