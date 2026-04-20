#!/bin/bash

# ====================================================================
# 🔄 EJECUTAR EN MÚLTIPLES VMs EN PARALELO
# ====================================================================
# Descarga y ejecuta el scraper en las 9 VMs simultáneamente
# ====================================================================

PROJECT_ID="guru-491919"
ZONE="us-central1-a"

# Array de VMs y sus tipos de scraper
VMS=(
    "lamudi-vm-1:bj_cu"
    "lamudi-vm-2:cdmx"
    "lamudi-vm-3:cdmx"
    "lamudi-vm-4:cdmx"
    "lamudi-vm-bj-cu:bj_cu"
    "lamudi-vm-cdmx:cdmx"
    "lamudi-vm-edomex:cdmx"
    "lamudi-vm-nl:cdmx"
    "lamudi-vm-ja-yuc-qroo:cdmx"
)

echo "═════════════════════════════════════════════════════════════"
echo "🔄 Iniciando scrapers en $(echo ${#VMS[@]}) VMs"
echo "═════════════════════════════════════════════════════════════"
echo ""

# Función para ejecutar en una VM
ejecutar_en_vm() {
    local vm_info="$1"
    local vm_name="${vm_info%%:*}"
    local script_type="${vm_info##*:}"
    
    echo "🚀 [$vm_name] Conectando e iniciando scraper ($script_type)..."
    
    gcloud compute ssh "$vm_name" \
        --project="$PROJECT_ID" \
        --zone="$ZONE" \
        --command="
            cd ~/lamudi_scrape || mkdir -p ~/lamudi_scrape && cd ~/lamudi_scrape
            git clone https://github.com/ai360Daniel/lamudi_scrape_vm.git . 2>/dev/null || git pull origin main
            source venv/bin/activate 2>/dev/null || python3 -m venv venv && source venv/bin/activate
            nohup python -u vm/lamudi_scraper_${script_type}.py > app_${script_type}.log 2>&1 &
            echo '✅ Scraper iniciado en background'
        " &
    
    sleep 2  # Evitar saturación de conexiones
}

# Ejecutar en paralelo
for vm_info in "${VMS[@]}"; do
    ejecutar_en_vm "$vm_info"
done

echo ""
echo "═════════════════════════════════════════════════════════════"
echo "✅ Todos los scrapers iniciados"
echo ""
echo "📊 MONITOREAR PROGRESO:"
echo ""
for vm_info in "${VMS[@]}"; do
    vm_name="${vm_info%%:*}"
    echo "   gcloud compute ssh $vm_name --zone=$ZONE --command='tail -f ~/lamudi_scrape/app_*.log'"
done
echo ""
echo "═════════════════════════════════════════════════════════════"
