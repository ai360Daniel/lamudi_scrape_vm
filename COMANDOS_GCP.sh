#!/bin/bash

# ====================================================================
# 🌐 COMANDOS GCP - DESPLIEGUE EN VMs
# ====================================================================
# Copia estos comandos y ejecútalos en tu terminal local con gcloud
# ====================================================================

PROJECT_ID="guru-491919"
ZONE="us-central1-a"
REPO_URL="https://github.com/ai360Daniel/lamudi_scrape_vm.git"

echo "📋 PASO 1: Copiar archivo de credenciales a la VM"
echo "═════════════════════════════════════════════════════════════"
echo "Comando:"
echo ""
echo "gcloud compute scp ai360-260519-3181376e2754.json lamudi-vm-bj-cu:/tmp/ \\"
echo "  --project=$PROJECT_ID --zone=$ZONE"
echo ""
echo ""

echo "📋 PASO 2: Conectar a la VM e instalar todo"
echo "═════════════════════════════════════════════════════════════"
echo "Comando:"
echo ""
echo "gcloud compute ssh lamudi-vm-bj-cu --project=$PROJECT_ID --zone=$ZONE --command='"
cat << 'EOF'
#!/bin/bash
set -e

# 1. Actualizar sistema
sudo apt-get update && sudo apt-get upgrade -y

# 2. Instalar dependencias
sudo apt-get install -y python3-venv python3-pip git chromium-browser chromium-driver

# 3. Crear directorio de trabajo
mkdir -p ~/lamudi_scrape && cd ~/lamudi_scrape

# 4. Clonar repositorio
git clone https://github.com/ai360Daniel/lamudi_scrape_vm.git . || git pull origin main

# 5. Crear y activar entorno virtual
python3 -m venv venv
source venv/bin/activate

# 6. Instalar paquetes Python
pip install --upgrade pip
pip install selenium webdriver-manager pandas google-cloud-storage geopandas fiona shapely requests

# 7. Copiar credenciales
cp /tmp/ai360-260519-3181376e2754.json ./

# 8. Ejecutar scraper
nohup python -u vm/lamudi_scraper_bj_cu.py > app.log 2>&1 &
echo "✅ Scraper iniciado"
'
EOF
echo ""
echo ""

echo "📋 PASO 3: Monitorear progreso"
echo "═════════════════════════════════════════════════════════════"
echo "Comando (ejecutar en terminal local):"
echo ""
echo "gcloud compute ssh lamudi-vm-bj-cu --project=$PROJECT_ID --zone=$ZONE --command='tail -f ~/lamudi_scrape/app.log'"
echo ""
echo ""

echo "📋 ALTERNATIVA: SCRIPT AUTOMÁTICO (TODO EN UNO)"
echo "═════════════════════════════════════════════════════════════"
echo "Guarda esto en un archivo llamado: deploy_vm.sh"
echo ""
cat << 'DEPLOY_SCRIPT'
#!/bin/bash

VM_NAME="lamudi-vm-bj-cu"
PROJECT_ID="guru-491919"
ZONE="us-central1-a"

# Subir credenciales
gcloud compute scp ai360-260519-3181376e2754.json $VM_NAME:/tmp/ \
  --project=$PROJECT_ID --zone=$ZONE

# Ejecutar instalación completa
gcloud compute ssh $VM_NAME --project=$PROJECT_ID --zone=$ZONE << 'EOF'
#!/bin/bash
set -e
echo "🚀 Iniciando instalación..."

sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3-venv python3-pip git chromium-browser chromium-driver

mkdir -p ~/lamudi_scrape && cd ~/lamudi_scrape
git clone https://github.com/ai360Daniel/lamudi_scrape_vm.git . 2>/dev/null || git pull origin main

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install selenium webdriver-manager pandas google-cloud-storage geopandas fiona shapely requests

cp /tmp/ai360-260519-3181376e2754.json ./
nohup python -u vm/lamudi_scraper_bj_cu.py > app.log 2>&1 &

echo "✅ Scraper iniciado. Monitorear con: tail -f app.log"
EOF

echo "✅ Despliegue completado"
DEPLOY_SCRIPT

echo ""
echo "EJECUTAR CON:"
echo "bash deploy_vm.sh"
echo ""
