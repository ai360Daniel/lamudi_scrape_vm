#!/bin/bash

set -e

REPO_DIR="lamudi_scrape_vm"

echo "🔄 Actualizando paquetes y dependencias base..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv git wget curl libgconf-2-4 libnss3 libx11-6 libasound2 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxshmfence1 libxtst6 ca-certificates fonts-liberation libappindicator3-1 lsb-release xdg-utils

echo "🌐 Instalando Google Chrome..."
if ! command -v google-chrome &> /dev/null; then
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo apt install -y ./google-chrome-stable_current_amd64.deb
    rm google-chrome-stable_current_amd64.deb
else
    echo "✅ Chrome ya está instalado"
fi

if [ -d "$REPO_DIR" ]; then
    echo "📂 Repo ya existe en $PWD, actualizando..."
    cd $REPO_DIR
    git reset --hard
    git pull
else
    echo "📦 Clonando repositorio..."
    git clone https://github.com/ai360Daniel/lamudi_scrape_vm.git
    cd $REPO_DIR
fi

echo "🐍 Configurando entorno virtual..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "🚀 Configuración de la VM completada con éxito."
