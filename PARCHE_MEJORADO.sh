#!/bin/bash

# 🔧 SCRIPT DE CONFIGURACIÓN DE VERSIÓN MEJORADA
# Este script parcha la función scrape_lamudi para incluir reintentos y reinicio del driver

SCRAPER_FILE="scraper_functions.py"

# Buscar y reemplazar la inicialización del driver para agregar timeout
sed -i 's/driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)/driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)\n    driver.set_page_load_timeout(30)\n    driver.implicitly_wait(10)/g' $SCRAPER_FILE

echo "✅ Configuración aplicada"
echo "   • Page load timeout: 30s"
echo "   • Implicit wait: 10s"
echo ""
echo "⚠️  NOTA: Para reintentos automáticos, ejecuta manualmente:"
echo "   nohup python -u lamudi_scraper_bj_cu.py > app.log 2>&1 &"
