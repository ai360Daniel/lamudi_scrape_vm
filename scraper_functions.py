"""
Lamudi Web Scraper - Funciones de Scraping y Procesamiento
Módulo con todas las funciones para descargar y procesar propiedades de Lamudi.com.mx
"""

import time
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from io import BytesIO

import pandas as pd
import numpy as np
from unidecode import unidecode
from shapely.geometry import Point
import geopandas as gpd
from dateutil import parser

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from google.cloud import storage



# ============================================================================
# CONFIGURACIÓN DE GOOGLE CLOUD STORAGE
# ============================================================================
# Configuración para ejecutarse en VM de GCP con service account asignada.
# Las credenciales se obtienen automáticamente mediante Application Default Credentials (ADC).
# No requiere variables de entorno o archivos de credenciales.

PROJECT_ID = "guru-491919"
BUCKET_NAME = "scraping_inmuebles"
GCS_FOLDER_PREFIX = "Lamudi"
# Prefijo de fecha personalizado para la carpeta (ej. '26_04_14')
CUSTOM_DATE_PREFIX = "26_04_14"

# Lista de estados para conteo
ESTADOS_CONFIG = [
    "aguascalientes", "baja-california", "baja-california-sur", "campeche", 
    "chiapas", "chihuahua", "coahuila-de-zaragoza", "colima", "distrito-federal", 
    "durango", "estado-de-mexico", "guanajuato", "guerrero", "hidalgo", 
    "jalisco", "michoacan", "morelos", "nayarit", "nuevo-leon", "oaxaca", 
    "puebla", "queretaro", "quintana-roo", "san-luis-potosi", "sinaloa", 
    "sonora", "tabasco", "tamaulipas", "tlaxcala", "veracruz", "yucatan", "zacatecas"
]

def obtener_carpeta_anio_mes():
    """
    Obtiene la carpeta con el prefijo GCS y la fecha (prioriza CUSTOM_DATE_PREFIX si existe).
    
    Returns:
        str: Ruta completa en GCS (Lamudi/YYYY_MM/ o Lamudi/CUSTOM_DATE_PREFIX/)
    """
    if CUSTOM_DATE_PREFIX:
        carpeta = CUSTOM_DATE_PREFIX
    else:
        ahora = datetime.now()
        carpeta = ahora.strftime("%Y_%m")
    
    return f"{GCS_FOLDER_PREFIX}/{carpeta}/"


def obtener_cliente_gcs():
    """
    Obtiene cliente de Google Cloud Storage.
    Utiliza la llave JSON local si existe, de lo contrario usa 
    Application Default Credentials (ADC).
    
    Returns:
        google.cloud.storage.Client: Cliente de GCS
    """
    path_key = "guru-491919-ec54091ec0b6.json"
    if os.path.exists(path_key):
        print(f"🔑 Usando llave JSON: {path_key}")
        return storage.Client.from_service_account_json(path_key, project=PROJECT_ID)
    
    print("📡 Usando Application Default Credentials (ADC)")
    return storage.Client(project=PROJECT_ID)


def crear_carpeta_gcs(ruta_carpeta):
    """
    Crea una carpeta en GCS si no existe (simulado con un archivo .folder).
    
    Args:
        ruta_carpeta (str): Ruta de la carpeta (ej: Lamudi/2024_03/)
    """
    try:
        cliente = obtener_cliente_gcs()
        bucket = cliente.bucket(BUCKET_NAME)
        blob = bucket.blob(ruta_carpeta + ".folder")
        if not blob.exists():
            blob.upload_from_string('')
            print(f"✅ Carpeta GCS creada: {ruta_carpeta}")
        else:
            print(f"ℹ️  Carpeta GCS ya existe, no se realizara accion: {ruta_carpeta}")
    except Exception as e:
        print(f"⚠️  No se pudo crear carpeta en GCS: {str(e)}")


def guardar_archivo_gcs(archivo_local, ruta_gcs):
    """
    Sube un archivo local a Google Cloud Storage solo si no existe.
    
    Args:
        archivo_local (str): Ruta del archivo local
        ruta_gcs (str): Ruta completa en GCS (Lamudi/YYYY_MM/nombre_archivo.csv)
        
    Returns:
        bool: True si se guardó exitosamente, False en caso de error o si ya existe
    """
    try:
        cliente = obtener_cliente_gcs()
        bucket = cliente.bucket(BUCKET_NAME)
        blob = bucket.blob(ruta_gcs)
        
        if blob.exists():
            print(f"⚠️  El archivo ya existe en GCS, NO se sobrescribira: {ruta_gcs}")
            return False
            
        blob.upload_from_filename(archivo_local)
        print(f"✅ Archivo guardado en GCS: {ruta_gcs}")
        return True
    except Exception as e:
        print(f"❌ Error guardando en GCS: {str(e)}")
        return False


def leer_archivo_gcs(ruta_gcs):
    """
    Lee un archivo CSV desde Google Cloud Storage.
    
    Args:
        ruta_gcs (str): Ruta completa en GCS (Lamudi/YYYY_MM/nombre_archivo.csv)
        
    Returns:
        pd.DataFrame: DataFrame con los datos del archivo
    """
    try:
        cliente = obtener_cliente_gcs()
        bucket = cliente.bucket(BUCKET_NAME)
        blob = bucket.blob(ruta_gcs)
        contenido = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(contenido))
        print(f"✅ Archivo leído desde GCS: {ruta_gcs}")
        return df
    except Exception as e:
        print(f"❌ Error leyendo archivo de GCS: {str(e)}")
        return pd.DataFrame()


def archivo_existe_gcs(ruta_gcs):
    """
    Verifica si un archivo existe en GCS.
    
    Args:
        ruta_gcs (str): Ruta completa en GCS
        
    Returns:
        bool: True si existe, False si no
    """
    try:
        cliente = obtener_cliente_gcs()
        bucket = cliente.bucket(BUCKET_NAME)
        blob = bucket.blob(ruta_gcs)
        return blob.exists()
    except Exception as e:
        print(f"⚠️  Error verificando archivo en GCS: {str(e)}")
        return False


# ============================================================================
# FUNCIONES DE CONFIGURACIÓN
# ============================================================================

def construir_url(estado, tipo_propiedad=None, accion='for-sale'):
    """
    Construye la URL de Lamudi dinámicamente.
    
    Args:
        estado (str): Estado de la URL (ej: 'aguascalientes')
        tipo_propiedad (str, optional): Tipo de propiedad (ej: 'comercial'). Si es None, obtiene todos.
        accion (str): Acción a realizar ('for-sale' o 'for-rent'). Default: 'for-sale'
    
    Returns:
        str: URL completa construida
    
    Ejemplos:
        >>> construir_url('aguascalientes')
        'https://www.lamudi.com.mx/aguascalientes/for-sale/'
        >>> construir_url('distrito-federal', 'comercial')
        'https://www.lamudi.com.mx/distrito-federal/comercial/for-sale/'
    """
    base_url = "https://www.lamudi.com.mx"
    
    if tipo_propiedad:
        url = f"{base_url}/{estado}/{tipo_propiedad}/{accion}/"
    else:
        url = f"{base_url}/{estado}/{accion}/"
    
    return url


def obtener_nombre_archivo(estado, tipo_propiedad=None, accion='for-sale'):
    """
    Genera nombre del archivo CSV basado en estado, tipo y acción.
    
    Args:
        estado (str): Estado
        tipo_propiedad (str, optional): Tipo de propiedad
        accion (str): Acción ('for-sale' o 'for-rent')
    
    Returns:
        str: Nombre del archivo CSV
    """
    if tipo_propiedad:
        filename = f"{estado}_{tipo_propiedad}_{accion.replace('-', '_')}.csv"
    else:
        filename = f"{estado}_{accion.replace('-', '_')}.csv"
    
    return filename


# ============================================================================
# FUNCIONES DE SCRAPING
# ============================================================================

def scrape_lamudi(start_url, output_filename, usar_gcs=True, max_paginas=None, reintentos=3, timeout=60, reinicio_driver_cada=8):
    """
    Descarga propiedades de Lamudi desde la URL especificada.
    
    Args:
        start_url (str): URL de inicio del scraping
        output_filename (str): Nombre del archivo CSV para guardar datos
        usar_gcs (bool): Si True, guarda en Google Cloud Storage (default True)
        max_paginas (int, optional): Número máximo de páginas a procesar para pruebas.
        
    Returns:
        int: Número de propiedades nuevas descargadas
    """
    # Configuración de Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Activado para VMs
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")  # 🔥 CRÍTICO para VMs sin suficiente memoria
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-web-resources")  # Reducir uso de memoria

    # Inicializar el driver
    try:
        # Intenta usar Selenium Manager (automático en Selenium 4+)
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print(f"⚠️ Error al inicializar Chrome con Selenium Manager: {e}")
        print("🔄 Intentando inicialización alternativa con ChromeDriverManager...")
        try:
            # Intento alternativo con ChromeDriverManager si el Manager falla
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except Exception as e2:
            print(f"❌ Error crítico al inicializar Chrome: {e2}")
            return 0

    driver.set_page_load_timeout(timeout)  # 🔥 Usar timeout pasado como parámetro (60s default)
    driver.implicitly_wait(15)  # Aumentado de 2 a 15
    
    # Lista para almacenar los datos
    data_list = []
    wait = WebDriverWait(driver, 15)  # Aumentado de 2 a 15
    total_propiedades = 0
    total_paginas = 0
    paginas_procesadas = 0
    
    # Configurar ruta GCS si está activado
    carpeta_gcs = obtener_carpeta_anio_mes() if usar_gcs else None
    ruta_gcs_completa = f"{carpeta_gcs}{output_filename}" if usar_gcs else None
    
    if usar_gcs:
        crear_carpeta_gcs(carpeta_gcs)
    
    # Cargar títulos existentes si el archivo existe
    titulos_existentes = set()
    if usar_gcs and archivo_existe_gcs(ruta_gcs_completa):
        try:
            df_existente = leer_archivo_gcs(ruta_gcs_completa)
            titulos_existentes = set(df_existente['titulo'].dropna().unique())
            print(f"📂 {len(titulos_existentes)} propiedades ya descargadas en GCS")
        except:
            print("⚠ Comenzando desde cero en GCS")
    elif not usar_gcs and os.path.exists(output_filename):
        try:
            df_existente = pd.read_csv(output_filename)
            titulos_existentes = set(df_existente['titulo'].dropna().unique())
            print(f"📂 {len(titulos_existentes)} propiedades ya descargadas")
        except:
            print("⚠ Comenzando desde cero")
    
    columnas = [
        "titulo", "direccion", "tipo", "tipo_vivienda", "categoria", "precio", "superficie", "superficie_terreno",
        "habitaciones", "banios", "caracteristica_propiedad", "amenidades", "caracteristicas", 
        "planta", "descripcion", "url", "script_content", "fecha_publicacion", "fecha_consulta", "url_imagen"
    ]
    
    propiedades_nuevas = 0
    propiedades_repetidas = 0
    propiedades_con_error = 0

    try:
        driver.get(start_url)

        try:
            total_text = driver.find_element(By.CSS_SELECTOR, "span[data-test='title-section-result-number']").text
            total_text = total_text.replace(',', '').strip()
            total_propiedades = int(total_text)
            total_paginas = (total_propiedades + 29) // 30
            print(f"📊 Total: {total_propiedades:,} | Páginas estimadas: {total_paginas}")
        except:
            print("⚠ No se pudo obtener total")
            total_propiedades = float('inf')
            total_paginas = float('inf')

        page_number = 1
        usar_boton = False  # Flag para intentar con botón si falla el método de página

        while True:
            paginas_procesadas += 1
            
            # Limitar por max_paginas si está definido
            if max_paginas and paginas_procesadas > max_paginas:
                print(f"🛑 Límite de prueba alcanzado: {max_paginas} páginas")
                break
            
            if paginas_procesadas > total_paginas:
                print(f"✓ Límite de páginas alcanzado ({total_paginas})")
                break

            # Estrategia 1: Intentar con ?page=N
            if not usar_boton:
                try:
                    if "?" in start_url:
                        page_url = start_url + f"&page={page_number}"
                    else:
                        page_url = start_url + f"?page={page_number}"
                    
                    print(f"\n🌐 P{paginas_procesadas}: Cargando...")
                    driver.get(page_url)
                    print(f"   ✅ Página cargada")
                    page_number += 1
                    
                except Exception as e:
                    print(f"⚠ Método ?page= falló. Intentando con botón...")
                    usar_boton = True
                    continue

            # Estrategia 2: Usar botón de siguiente página (fallback)
            if usar_boton:
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "a#pagination-next")
                    next_url = next_button.get_attribute("href")
                    if next_url:
                        print(f"\n🌐 P{paginas_procesadas}: Siguiendo botón...")
                        driver.get(next_url)
                        print(f"   ✅ Página cargada")
                    else:
                        print(f"✓ Sin página siguiente. Fin en página {paginas_procesadas}")
                        break
                except NoSuchElementException:
                    print(f"✓ Última página alcanzada ({paginas_procesadas})")
                    break
                except Exception as e:
                    print(f"⚠ Error en navegación: {str(e)[:50]}. Finalizando...")
                    break

            try:
                print(f"   🔍 Buscando propiedades...")
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".snippet__content a[target]")))
                
                links = driver.find_elements(By.CSS_SELECTOR, ".snippet__content a[target]")
                print(f"   📌 Links DOM encontrados: {len(links)}")
                
                property_links = [link.get_attribute("href") for link in links if link.get_attribute("href")]
                print(f"   ✔️ Links válidos (con href): {len(property_links)}")

                if not property_links:
                    print(f"✓ Sin propiedades. Fin en página {paginas_procesadas}")
                    break

                print(f"   📋 Procesando {len(property_links)} propiedades...")

                propiedades_nuevas_pagina = 0
                propiedades_repetidas_pagina = 0
                propiedades_error_pagina = 0

                for idx, property_url in enumerate(property_links):
                    try:
                        driver.get(property_url)

                        propiedad = {}

                        try:
                            propiedad['titulo'] = driver.find_element(By.TAG_NAME, "h1").text
                        except:
                            propiedad['titulo'] = None
                        
                        if propiedad['titulo'] and propiedad['titulo'] in titulos_existentes:
                            propiedades_repetidas += 1
                            propiedades_repetidas_pagina += 1
                            print(f"      ⚠️  {idx+1}/30 REPETIDA: {propiedad['titulo'][:35]}...")
                            continue
                        
                        try:
                            propiedad['direccion'] = driver.find_element(By.CSS_SELECTOR, "div.location-map__location-address-map").text
                        except:
                            propiedad['direccion'] = None
                        try:
                            propiedad['tipo'] = driver.find_element(By.CSS_SELECTOR, ".property-type span.place-features__values").text
                        except:
                            propiedad['tipo'] = None
                        try:
                            propiedad['tipo_vivienda'] = driver.find_element(By.CSS_SELECTOR, "span[data-test='property-type-value']").text
                        except:
                            propiedad['tipo_vivienda'] = None
                        
                        propiedad['categoria'] = propiedad['tipo_vivienda']
                        
                        try:
                            propiedad['precio'] = driver.find_element(By.CSS_SELECTOR, "div.prices-and-fees__price").text
                        except:
                            propiedad['precio'] = None
                        try:
                            propiedad['superficie'] = driver.find_element(By.CSS_SELECTOR, "div.details-item-value[data-test='area-value']").text
                        except:
                            propiedad['superficie'] = None
                        try:
                            propiedad['superficie_terreno'] = driver.find_element(By.CSS_SELECTOR, "span[data-test='plot-area-value']").text
                        except:
                            propiedad['superficie_terreno'] = None
                        try:
                            propiedad['habitaciones'] = driver.find_element(By.CSS_SELECTOR, "div.details-item-value[data-test='bedrooms-value']").text
                        except:
                            propiedad['habitaciones'] = None
                        try:
                            propiedad['banios'] = driver.find_element(By.CSS_SELECTOR, "div.details-item-value[data-test='full-bathrooms-value']").text
                        except:
                            propiedad['banios'] = None
                        try:
                            propiedad['caracteristica_propiedad'] = driver.find_element(By.CSS_SELECTOR, "div.facilities:nth-of-type(4)").text
                        except:
                            propiedad['caracteristica_propiedad'] = None
                        try:
                            propiedad['amenidades'] = driver.find_element(By.CSS_SELECTOR, "div.facilities:nth-of-type(5)").text
                        except:
                            propiedad['amenidades'] = None
                        try:
                            propiedad['caracteristicas'] = driver.find_element(By.CSS_SELECTOR, "div.place-details").text
                        except:
                            propiedad['caracteristicas'] = None
                        try:
                            propiedad['planta'] = driver.find_element(By.CSS_SELECTOR, ".floor span.place-features__values").text
                        except:
                            propiedad['planta'] = None
                        try:
                            propiedad['descripcion'] = driver.find_element(By.CSS_SELECTOR, "div#description-text").text
                        except:
                            propiedad['descripcion'] = None
                        try:
                            propiedad['fecha_publicacion'] = driver.find_element(By.CSS_SELECTOR, "div.date").text
                        except:
                            propiedad['fecha_publicacion'] = None
                        try:
                            propiedad['url_imagen'] = driver.find_element(By.CSS_SELECTOR, "div.swiper-wrapper img").get_attribute("src")
                        except:
                            propiedad['url_imagen'] = None
                        
                        propiedad['script_content'] = None
                        try:
                            if "mapData" in driver.page_source:
                                match = re.search(r'<script[^>]*>(.*?mapData.*?adLocationData.*?)</script>', driver.page_source, re.DOTALL)
                                if match:
                                    propiedad['script_content'] = match.group(1)
                        except:
                            pass
                        
                        propiedad['url'] = property_url
                        propiedad['fecha_consulta'] = time.strftime('%Y-%m-%d')

                        data_list.append(propiedad)
                        titulos_existentes.add(propiedad['titulo'])
                        propiedades_nuevas += 1
                        propiedades_nuevas_pagina += 1
                        print(f"      ✅ {idx+1}/30 OK ({propiedades_nuevas} total): {propiedad['titulo'][:30]}...")

                    except Exception as e:
                        propiedades_con_error += 1
                        propiedades_error_pagina += 1
                        print(f"      ❌ {idx+1}/30 ERROR: {str(e)[:45]}")
                        continue

                # RESUMEN DE PÁGINA
                print(f"\n   📊 RESUMEN P{paginas_procesadas}:")
                print(f"      • Links encontrados: {len(property_links)}")
                print(f"      • Nuevas descargadas: {propiedades_nuevas_pagina}")
                print(f"      • Repetidas saltadas: {propiedades_repetidas_pagina}")
                print(f"      • Con errores: {propiedades_error_pagina}")
                print(f"      • Procesadas total: {propiedades_nuevas_pagina + propiedades_repetidas_pagina + propiedades_error_pagina}")
                
                # Guardar incrementalmente
                if data_list:
                    data_rows = [[prop.get(col) for col in columnas] for prop in data_list]
                    df_nuevo = pd.DataFrame(data_rows, columns=columnas)
                    
                    if usar_gcs:
                        # Guardar en GCS
                        try:
                            cliente = obtener_cliente_gcs()
                            bucket = cliente.bucket(BUCKET_NAME)
                            blob = bucket.blob(ruta_gcs_completa)
                            
                            # Leer archivo existente en GCS
                            if blob.exists():
                                contenido = blob.download_as_bytes()
                                df_existente = pd.read_csv(BytesIO(contenido))
                                # Concatenar nuevas propiedades a las existentes
                                df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
                                print(f"      📝 Archivo existe en GCS. Se añadieron {len(df_nuevo)} filas nuevas.")
                            else:
                                df_final = df_nuevo
                                print(f"      🆕 Creando nuevo archivo en GCS.")
                            
                            # Guardar en GCS (siempre sube la versión completa actualizada)
                            csv_buffer = BytesIO()
                            df_final.to_csv(csv_buffer, index=False, encoding='utf-8')
                            csv_buffer.seek(0)
                            blob.upload_from_file(csv_buffer, content_type='text/csv')
                            print(f"      💾 Actualizado en GCS: {ruta_gcs_completa}\n")
                        except Exception as e:
                            print(f"      ❌ Error guardando en GCS: {str(e)}\n")
                    else:
                        # Guardar localmente
                        mode = 'a' if os.path.exists(output_filename) else 'w'
                        header = not os.path.exists(output_filename)
                        df_nuevo.to_csv(output_filename, mode='a' if mode == 'a' else 'w', header=header if mode == 'w' else False, index=False, encoding="utf-8")
                        print(f"      💾 Guardadas en archivo\n")
                    
                    data_list = []
                
                # 🔥 REINICIO DEL DRIVER CADA X PÁGINAS (soluciona timeout)
                if paginas_procesadas % reinicio_driver_cada == 0 and paginas_procesadas > 0:
                    print(f"\n🔄 REINICIANDO DRIVER EN PÁGINA {paginas_procesadas}...")
                    try:
                        driver.quit()
                        time.sleep(2)
                        # Reinicializar driver con mismas opciones
                        driver = webdriver.Chrome(options=chrome_options)
                        driver.set_page_load_timeout(timeout)
                        driver.implicitly_wait(15)
                        wait = WebDriverWait(driver, 15)
                        print(f"✅ Driver reiniciado correctamente\n")
                    except Exception as e:
                        print(f"⚠️  Error reiniciando driver: {str(e)[:50]}")
                
                # ⏳ PAUSA ENTRE PÁGINAS (evita saturación del servidor)
                time.sleep(3)

            except Exception as e:
                print(f"Error de página: {str(e)[:80]}")
                # Reintentar la página si falla
                if paginas_procesadas <= reintentos:
                    print(f"🔄 Reintentando página {paginas_procesadas} ({paginas_procesadas}/{reintentos})...")
                    time.sleep(5)
                    continue
                else:
                    break

    except Exception as e:
        print(f"Error crítico: {str(e)}")
    
    finally:
        driver.quit()

    print(f"\n{'='*80}")
    print(f"✅ DESCARGA COMPLETADA")
    print(f"{'='*80}")
    print(f"   ✅ Nuevas descargadas: {propiedades_nuevas}")
    print(f"   ⚠️  Repetidas saltadas: {propiedades_repetidas}")
    print(f"   ❌ Con errores: {propiedades_con_error}")
    print(f"   📊 Total procesadas: {propiedades_nuevas + propiedades_repetidas + propiedades_con_error}")
    print(f"{'='*80}\n")
    return propiedades_nuevas


# ============================================================================
# FUNCIONES PARA MANEJAR LINKS FALLIDOS
# ============================================================================

def guardar_links_fallidos(output_filename, failed_links, usar_gcs=True):
    """
    Guarda los links que fallaron en un archivo JSON para reintentar después.
    
    Args:
        output_filename (str): Nombre base del CSV (ej: 'aguascalientes_casa.csv')
        failed_links (list): Lista de dicts con links fallidos
        usar_gcs (bool): Si True, guarda en Google Cloud Storage
    """
    if not failed_links:
        print("✅ No hay links fallidos para guardar")
        return None
    
    base_name = output_filename.replace('.csv', '')
    failed_filename = f"{base_name}_failed_links.json"
    
    datos_fallidos = {
        "timestamp": datetime.now().isoformat(),
        "total_fallidos": len(failed_links),
        "links": failed_links
    }
    
    if usar_gcs:
        # Guardar en GCS
        carpeta_gcs = obtener_carpeta_anio_mes()
        ruta_gcs = f"{carpeta_gcs}{failed_filename}"
        try:
            cliente = obtener_cliente_gcs()
            bucket = cliente.bucket(BUCKET_NAME)
            blob = bucket.blob(ruta_gcs)
            contenido_json = json.dumps(datos_fallidos, ensure_ascii=False, indent=2)
            blob.upload_from_string(contenido_json, content_type='application/json')
            print(f"\n💾 LINKS FALLIDOS GUARDADOS EN GCS:")
            print(f"   📁 Archivo: {ruta_gcs}")
            print(f"   📊 Total: {len(failed_links)} links fallidos")
            print(f"   ⏰ Timestamp: {datos_fallidos['timestamp']}")
        except Exception as e:
            print(f"   ❌ Error guardando en GCS: {str(e)}")
    else:
        # Guardar localmente
        with open(failed_filename, 'w', encoding='utf-8') as f:
            json.dump(datos_fallidos, f, ensure_ascii=False, indent=2)
        print(f"\n💾 LINKS FALLIDOS GUARDADOS:")
        print(f"   📁 Archivo: {failed_filename}")
        print(f"   📊 Total: {len(failed_links)} links fallidos")
        print(f"   ⏰ Timestamp: {datos_fallidos['timestamp']}")
    
    print(f"   🔍 Detalles por razón:")
    
    razones = {}
    for link in failed_links:
        razon = link.get('razon', 'Unknown')
        razones[razon] = razones.get(razon, 0) + 1
    
    for razon, count in sorted(razones.items()):
        print(f"      • {razon}: {count} fallos")
    
    return failed_filename


def reintentar_links_fallidos(failed_filename, nuevo_csv=None):
    """
    Reintenta descargar los links que fallaron anteriormente.
    
    Args:
        failed_filename (str): Ruta del archivo JSON con links fallidos
        nuevo_csv (str, optional): Nombre del CSV para guardar nuevas propiedades
    
    Returns:
        str: Ruta del archivo con links que siguen fallando, o None si todos se descargaron
    """
    print(f"\n🔄 REINTENTANDO LINKS FALLIDOS")
    print("=" * 80)
    
    with open(failed_filename, 'r', encoding='utf-8') as f:
        datos = json.load(f)
    
    failed_links = datos.get('links', [])
    total_fallidos = len(failed_links)
    
    print(f"📂 Cargados {total_fallidos} links de: {failed_filename}")
    print(f"⏰ Original: {datos.get('timestamp', 'N/A')}")
    print("=" * 80)
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    except Exception as e:
        print(f"⚠️ Error al inicializar Chrome en reintento: {e}")
        try:
            driver = webdriver.Chrome(options=chrome_options)
        except Exception as e2:
            print(f"❌ Falló inicialización de Chrome en reintento: {e2}")
            return False

    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)
    wait = WebDriverWait(driver, 10)
    
    exitosos = 0
    seguidos_fallando = []
    
    try:
        for idx, link_data in enumerate(failed_links, 1):
            property_url = link_data.get('url')
            razon_original = link_data.get('razon', 'Unknown')
            
            try:
                print(f"🔄 {idx}/{total_fallidos} Reintentando ({razon_original[:25]}...)...", end="")
                driver.get(property_url)
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ad-details")))
                
                print(" ✅ EXITOSO")
                exitosos += 1
                
            except TimeoutException:
                print(f" ⏱️  Timeout")
                seguidos_fallando.append({
                    **link_data,
                    "reintento": "timeout"
                })
            except Exception as e:
                print(f" ❌ {str(e)[:30]}")
                seguidos_fallando.append({
                    **link_data,
                    "reintento": str(e)[:60]
                })
    
    finally:
        driver.quit()
    
    print("\n" + "=" * 80)
    print(f"📊 RESUMEN DE REINTENTOS:")
    print(f"   ✅ Exitosos: {exitosos}/{total_fallidos}")
    print(f"   ❌ Siguen fallando: {len(seguidos_fallando)}")
    print(f"   📈 Tasa éxito: {(exitosos/total_fallidos)*100:.1f}%")
    print("=" * 80)
    
    if seguidos_fallando:
        new_failed_filename = failed_filename.replace('.json', '_reintento.json')
        with open(new_failed_filename, 'w', encoding='utf-8') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "original_timestamp": datos.get('timestamp'),
                "exitosos": exitosos,
                "seguidos_fallando": len(seguidos_fallando),
                "links": seguidos_fallando
            }, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Links que siguen fallando guardados en: {new_failed_filename}")
        return new_failed_filename
    
    return None


def scrape_y_guardar_fallidos(start_url, output_filename, usar_gcs=True, max_paginas=None, **kwargs):
    """
    Ejecuta scraping y guarda automáticamente los links fallidos en JSON.
    
    Args:
        start_url (str): URL de inicio
        output_filename (str): Nombre del archivo CSV de salida
        usar_gcs (bool): Si True, guarda en Google Cloud Storage
        max_paginas (int, optional): Número máximo de páginas para pruebas.
        
    Returns:
        list: Lista de links fallidos
    """
    print(f"🌐 Iniciando descarga desde: {start_url}")
    
    failed_links = []
    
    # Pasar parámetros de reintentos, timeout e reinicio del driver
    scrape_lamudi(start_url, output_filename, usar_gcs=usar_gcs, max_paginas=max_paginas, **kwargs)
    
    if failed_links:
        guardar_links_fallidos(output_filename, failed_links, usar_gcs=usar_gcs)
    else:
        print("✅ Se descargaron todas las propiedades sin errores")
    
    return failed_links


# ============================================================================
# FUNCIONES DE LIMPIEZA Y PROCESAMIENTO DE DATOS
# ============================================================================

def limpiar_df(output_filename, usar_gcs=True):
    """
    Limpia y procesa el DataFrame descargado.
    
    Procesa:
    - Eliminación de textos duplicados
    - Extracción de características (amenidades, estacionamiento, etc.)
    - Extracción de coordenadas geográficas
    - Conversión de datos numéricos
    - Enriquecimiento con información geográfica (CP y colonias)
    - Conversión de fechas
    
    Args:
        output_filename (str): Ruta del archivo CSV
        usar_gcs (bool): Si True, lee y guarda en Google Cloud Storage
        
    Returns:
        pd.DataFrame: DataFrame limpio y procesado
    """
    # Leer datos
    if usar_gcs:
        carpeta_gcs = obtener_carpeta_anio_mes()
        ruta_gcs = f"{carpeta_gcs}{output_filename}"
        df = leer_archivo_gcs(ruta_gcs)
        archivo_referencia = ruta_gcs
    else:
        df = pd.read_csv(output_filename)
        archivo_referencia = output_filename
    
    print(f"✅ Leyendo {len(df)} propiedades de {archivo_referencia}")
    print(f"   Categorías presentes: {df['categoria'].value_counts().to_dict()}")
    
    # Eliminar textos duplicados
    df['caracteristica_propiedad'] = df['caracteristica_propiedad'].str.replace('^Características de la propiedad\n', '', regex=True)
    df['amenidades'] = df['amenidades'].str.replace('^Características del edificio\n', '', regex=True)
    df[['fecha_publicacion', 'publicado_por']] = df['fecha_publicacion'].str.split(' - Publicado por ', expand=True)

    # Normalización de texto
    df['amenidades'] = df['amenidades'].apply(lambda x: unidecode(x).lower() if pd.notna(x) else x)

    # Extracción de características
    df['estacionamiento'] = df['caracteristica_propiedad'].str.contains('estacionamiento', case=False, na=False).astype(int)
    df['alberca'] = df['amenidades'].str.contains('alberca', case=False, na=False).astype(int)
    df['seguridad'] = df['amenidades'].str.contains('seguridad', case=False, na=False).astype(int)
    df['gimnasio'] = df['amenidades'].str.contains('gimnasio', case=False, na=False).astype(int)
    df['elevador'] = df['amenidades'].str.contains('elevador', case=False, na=False).astype(int)
    df['roof_garden'] = df['amenidades'].str.contains('roof', case=False, na=False).astype(int)
    df['jardin'] = df['amenidades'].str.contains('jardin', case=False, na=False).astype(int)
    df['salon'] = df['amenidades'].str.contains('salon', case=False, na=False).astype(int)
    df['mascotas'] = df['amenidades'].str.contains('pet', case=False, na=False).astype(int)
    
    # Búsqueda secundaria en descripción
    df['alberca'] = np.where(df['alberca'] == 0, 
                            df['descripcion'].str.contains('alberca', case=False, na=False).astype(int), 
                            df['alberca'])
    df['seguridad'] = np.where(df['seguridad'] == 0, 
                                df['descripcion'].str.contains('vigilancia|seguridad', case=False, na=False).astype(int), 
                                df['seguridad'])
    df['gimnasio'] = np.where(df['gimnasio'] == 0, 
                            df['descripcion'].str.contains('gimnasio', case=False, na=False).astype(int), 
                            df['gimnasio'])
    df['elevador'] = np.where(df['elevador'] == 0, 
                            df['descripcion'].str.contains('elevador', case=False, na=False).astype(int), 
                            df['elevador'])
    df['roof_garden'] = np.where(df['roof_garden'] == 0, 
                                df['descripcion'].str.contains('roof', case=False, na=False).astype(int), 
                                df['roof_garden'])
    df['mascotas'] = np.where(df['mascotas'] == 0, 
                                df['descripcion'].str.contains('mascotas', case=False, na=False).astype(int), 
                                df['mascotas'])
    df['salon'] = np.where(df['salon'] == 0, 
                                df['descripcion'].str.contains('salon', case=False, na=False).astype(int), 
                                df['salon'])

    # Extracción de coordenadas geográficas
    df['latitud'] = df['script_content'].str.extract(r'latitude:(.{9})', expand=False)
    df['latitud'] = df['latitud'].replace(r'[^0-9.-]', '', regex=True)
    df['latitud'] = pd.to_numeric(df['latitud'], errors='coerce')

    df['longitud'] = df['script_content'].str.extract(r'longitude:(.{10})', expand=False)
    df['longitud'] = df['longitud'].replace(r'[^0-9.-]', '', regex=True)
    df['longitud'] = pd.to_numeric(df['longitud'], errors='coerce')

    df['direccion'] = df['script_content'].str.extract(r'address:(.*?)\n', expand=False)
    df['direccion'] = df['direccion'].str.replace('`', '', regex=False)
    df['colonia'] = df['direccion'].str.split(',', n=2).str[1].str.strip()

    # Conversión a números
    df['superficie'] = pd.to_numeric(df['superficie'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')
    
    if 'superficie_terreno' in df.columns:
        df['superficie_terreno'] = pd.to_numeric(df['superficie_terreno'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')

    df['precio'] = pd.to_numeric(df['precio'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    df['habitaciones'] = pd.to_numeric(df['habitaciones'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')
    df['banios'] = pd.to_numeric(df['banios'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')

    # Enriquecimiento geográfico - CP (requiere archivos shapefiles)
    try:
        ruta_shp = "cp_cdmx/CP_09CDMX_v7"
        gdf = gpd.read_file(ruta_shp + ".shp")
        
        if not gdf.empty:
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:6365", allow_override=True)
            gdf = gdf.to_crs("EPSG:4326")
            
            geometry = [Point(lon, lat) for lon, lat in zip(df['longitud'], df['latitud'])]
            gdf_puntos = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            df_con_cp = gpd.sjoin(gdf_puntos, gdf, how="left", predicate='within')
            df_con_cp['cp'] = df_con_cp['d_cp']
            df = df_con_cp
    except Exception as e:
        print(f"⚠️  No se pudo cargar información de CP: {str(e)}")

    # Enriquecimiento geográfico - Colonias
    try:
        ruta_shp = "coloniascdmx/colonias_iecm"
        gdf = gpd.read_file(ruta_shp + ".shp")
        
        if not gdf.empty:
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:6365", allow_override=True)
            gdf = gdf.to_crs("EPSG:4326")
            
            geometry = [Point(lon, lat) for lon, lat in zip(df['longitud'], df['latitud'])]
            gdf_puntos = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            df_con_col = gpd.sjoin(gdf_puntos, gdf, how="left", predicate='within')
            df_con_col['colonia'] = df_con_col['NOMUT']
            df = df_con_col
    except Exception as e:
        print(f"⚠️  No se pudo cargar información de colonias: {str(e)}")

    # Conversión de fechas
    def convertir_fecha(fecha_str):
        if pd.isna(fecha_str):
            return None
            
        ahora = datetime.now()
        
        if re.search(r'\d{1,2} \w{3,} \d{4}', str(fecha_str)):
            fecha_abs = pd.to_datetime(fecha_str, format='%d %b %Y', errors='coerce')
            return fecha_abs if pd.notna(fecha_abs) else fecha_str
        
        match = re.findall(r'(\d+) (\w+)', str(fecha_str))
        if match:
            for cantidad, unidad in match:
                cantidad = int(cantidad)
                if 'hora' in unidad:
                    ahora -= timedelta(hours=cantidad)
                elif 'día' in unidad:
                    ahora -= timedelta(days=cantidad)
                elif 'semana' in unidad:
                    ahora -= timedelta(weeks=cantidad)
            return ahora

        return fecha_str

    df['fecha_publicacion'] = df['fecha_publicacion'].apply(convertir_fecha)
    
    def convertir_fecha_formato(fecha):
        try:
            return parser.parse(str(fecha), dayfirst=True).strftime('%d/%m/%Y')
        except ValueError:
            return None

    df['fecha_publicacion'] = df['fecha_publicacion'].apply(convertir_fecha_formato)

    # Eliminar columnas auxiliares
    cols_eliminar = ['NOMUT', 'ID', 'CVEUT', 'DTTOLOC', 'index_right', 'geometry', 'script_content', 'caracteristicas', 'amenidades', 'caracteristica_propiedad']
    cols_a_eliminar = [col for col in cols_eliminar if col in df.columns]
    df.drop(columns=cols_a_eliminar, inplace=True)

    print(f"✅ Limpieza completada. Dataset final: {len(df)} propiedades")
    
    return df


# ============================================================================
# FUNCIONES DE ANÁLISIS Y FILTRADO
# ============================================================================

def filtrar_por_categoria(df_limpio, categoria):
    """
    Filtra el dataset por categoría de propiedad.
    
    Args:
        df_limpio (pd.DataFrame): DataFrame limpio
        categoria (str): Categoría a filtrar
        
    Returns:
        pd.DataFrame: DataFrame filtrado
    """
    return df_limpio[df_limpio['categoria'] == categoria].copy()


def contar_propiedades_por_estado_y_tipo(transaccion='for-sale'):
    """
    Busca en Lamudi el número de propiedades por TIPO y por ESTADO.
    
    Args:
        transaccion (str): 'for-sale' (venta) o 'for-rent' (renta)
        
    Returns:
        pd.DataFrame: tabla con estados en filas, tipos de propiedad en columnas
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    except Exception as e:
        print(f"⚠️ Error al inicializar Chrome en conteo: {e}")
        try:
            driver = webdriver.Chrome(options=chrome_options)
        except Exception as e2:
            print(f"❌ Falló inicialización de Chrome en conteo: {e2}")
            return pd.DataFrame()

    driver.set_page_load_timeout(30)
    wait = WebDriverWait(driver, 10)
    
    tipos_propiedad = ['comercial', 'terreno', 'casa', 'departamento', 'offices']
    resultados = {estado: {tipo: 0 for tipo in tipos_propiedad} for estado in ESTADOS_CONFIG}
    
    print(f"🔍 Contando propiedades por TIPO y ESTADO ({transaccion})...")
    print("=" * 80)
    
    total_requests = len(tipos_propiedad) * len(ESTADOS_CONFIG)
    request_actual = 0
    
    try:
        for tipo in tipos_propiedad:
            print(f"\n📦 Procesando tipo: {tipo.upper()}")
            print("-" * 80)
            
            for idx, estado in enumerate(ESTADOS_CONFIG, 1):
                request_actual += 1
                try:
                    url = f"https://www.lamudi.com.mx/{estado}/{tipo}/{transaccion}/"
                    driver.get(url)
                    time.sleep(0.8)
                    
                    try:
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[data-test='title-section-result-number']")))
                        total_text = driver.find_element(By.CSS_SELECTOR, "span[data-test='title-section-result-number']").text
                        total_text = total_text.replace(',', '').strip()
                        total = int(total_text)
                        resultados[estado][tipo] = total
                        pct = (request_actual / total_requests) * 100
                        print(f"  [{pct:>5.1f}%] {estado:25s}: {total:>6,d} ✓")
                        
                    except:
                        resultados[estado][tipo] = 0
                        pct = (request_actual / total_requests) * 100
                        print(f"  [{pct:>5.1f}%] {estado:25s}: sin resultados")
                    
                except Exception as e:
                    print(f"  [{estado:25s}]: Error ({str(e)[:25]})")
                    resultados[estado][tipo] = 0
        
        df_resultado = pd.DataFrame(resultados).T
        df_resultado.columns = [col.upper() for col in df_resultado.columns]
        df_resultado['TOTAL'] = df_resultado.sum(axis=1)
        
        print("\n" + "=" * 80)
        print("📊 RESUMEN POR ESTADO Y TIPO DE PROPIEDAD")
        print("=" * 80)
        print(df_resultado.to_string())
        
        print("\n" + "=" * 80)
        print("📈 TOTALES POR TIPO DE PROPIEDAD")
        print("=" * 80)
        totales_tipo = df_resultado.drop('TOTAL', axis=1).sum()
        for tipo, count in totales_tipo.items():
            print(f"  {tipo:15s}: {count:>10,d} propiedades")
        
        print("\n" + "=" * 80)
        print(f"✅ TOTAL GENERAL: {df_resultado['TOTAL'].sum():,d} propiedades en México")
        print(f"📈 Promedio por estado: {int(df_resultado['TOTAL'].mean()):,d} propiedades")
        estado_max = df_resultado['TOTAL'].idxmax()
        print(f"🏆 Estado con más propiedades: {estado_max} ({df_resultado.loc[estado_max, 'TOTAL']:.0f})")
        print("=" * 80)
        
        return df_resultado
        
    finally:
        driver.quit()
