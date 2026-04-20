#!/bin/bash

# ====================================================================
# 🚀 SCRIPT DE INSTALACIÓN Y EJECUCIÓN COMPLETA - LAMUDI SCRAPER
# ====================================================================
# Este script instala todas las dependencias, descarga el código,
# y ejecuta el scraper mejorado en la VM
# ====================================================================

set -e  # Detener si hay error

echo "╔════════════════════════════════════════════════════════════╗"
echo "║  🚀 INICIANDO CONFIGURACIÓN DE LAMUDI SCRAPER VM           ║"
echo "╚════════════════════════════════════════════════════════════╝"

# ====================================================================
# 1️⃣  ACTUALIZAR SISTEMA
# ====================================================================
echo ""
echo "📦 [1/8] Actualizando sistema..."
sudo apt-get update
sudo apt-get upgrade -y

# ====================================================================
# 2️⃣  INSTALAR DEPENDENCIAS DEL SISTEMA
# ====================================================================
echo ""
echo "📦 [2/8] Instalando dependencias del sistema..."
sudo apt-get install -y \
    python3.10 \
    python3-pip \
    python3-venv \
    git \
    wget \
    curl \
    unzip \
    gnupg \
    chromium-browser \
    chromium-driver

# ====================================================================
# 3️⃣  CREAR DIRECTORIO DE TRABAJO
# ====================================================================
echo ""
echo "📁 [3/8] Creando directorio de trabajo..."
cd ~
mkdir -p lamudi_scrape_vm
cd lamudi_scrape_vm

# ====================================================================
# 4️⃣  CLONAR O ACTUALIZAR REPOSITORIO
# ====================================================================
echo ""
echo "📥 [4/8] Descargando repositorio desde GitHub..."

if [ -d ".git" ]; then
    echo "   📂 Repositorio ya existe, actualizando..."
    git pull origin main
else
    echo "   🆕 Clonando repositorio..."
    git clone https://github.com/ai360Daniel/lamudi_scrape_vm.git .
fi

# ====================================================================
# 5️⃣  CREAR ENTORNO VIRTUAL
# ====================================================================
echo ""
echo "🐍 [5/8] Creando entorno virtual Python..."

if [ -d "venv" ]; then
    echo "   ✅ Entorno virtual ya existe"
else
    python3 -m venv venv
fi

source venv/bin/activate

# ====================================================================
# 6️⃣  INSTALAR DEPENDENCIAS PYTHON
# ====================================================================
echo ""
echo "📦 [6/8] Instalando paquetes Python..."

pip install --upgrade pip setuptools wheel
pip install \
    selenium \
    webdriver-manager \
    pandas \
    google-cloud-storage \
    geopandas \
    fiona \
    shapely \
    requests

echo "   ✅ Todos los paquetes instalados"

# ====================================================================
# 7️⃣  DESCARGAR CREDENCIALES DE GCS (IMPORTANTE)
# ====================================================================
echo ""
echo "🔑 [7/8] Configurando credenciales de GCS..."

cat > gcs_credentials.json << 'EOF'
{
  "type": "service_account",
  "project_id": "guru-491919",
  "private_key_id": "ec54091ec0b6",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA2x8h7P...[COMPLETA TU KEY AQUÍ]\n-----END RSA PRIVATE KEY-----\n",
  "client_email": "lamudi-scraper@guru-491919.iam.gserviceaccount.com",
  "client_id": "1234567890",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
}
EOF

echo "   ⚠️  IMPORTANTE: Descarga el archivo JSON con las credenciales"
echo "      y cópialo en esta carpeta como: gcs_credentials.json"

# ====================================================================
# 8️⃣  EJECUTAR SCRAPER
# ====================================================================
echo ""
echo "🎯 [8/8] Iniciando scraper..."
echo ""
echo "═════════════════════════════════════════════════════════════"
echo "   Ejecutando: lamudi_scraper_bj_cu.py"
echo "   Rango: Benito Juárez + Cuauhtémoc-2"
echo "   Precios: 0-3M, 3M-6M, 6M+"
echo "   Logs en: app.log"
echo "═════════════════════════════════════════════════════════════"
echo ""

# Ejecutar en background con logs
nohup python -u vm/lamudi_scraper_bj_cu.py > app.log 2>&1 &

echo "✅ Scraper iniciado en background"
echo ""
echo "📊 MONITOREAR PROGRESO:"
echo "   tail -f app.log"
echo ""
echo "⏸️  DETENER SCRAPER:"
echo "   pkill -f lamudi_scraper_bj_cu.py"
echo ""
echo "✨ Configuración completada"
