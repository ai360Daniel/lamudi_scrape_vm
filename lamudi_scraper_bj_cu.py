"""
Lamudi Web Scraper - Script Principal
Herramienta para descargar y procesar propiedades de Lamudi.com.mx
Guarda datos automáticamente en Google Cloud Storage (scraping_inmuebles/Lamudi/YYYY_MM/)

Uso:
    python lamudi_scraper.py

Configuración en VM de GCP:
    1. Crear VM Compute Engine en proyecto: guru-491919
    2. Crear/asignar service account a la VM
    3. Dar permisos necesarios a la service account:
       - storage.buckets.get (scraping_inmuebles)
       - storage.objects.create (scraping_inmuebles/Lamudi/*)
       - storage.objects.delete (para actualizar archivos)
    4. El código usará automáticamente Application Default Credentials (ADC)
    5. Ejecutar: python lamudi_scraper.py

No requiere:
    - Variables de entorno GOOGLE_APPLICATION_CREDENTIALS
    - Archivos .json de credenciales
    - Instalación manual de credenciales
"""


# ============================================================================
# CONFIGURACIÓN ESPECÍFICA CDMX
# ============================================================================

ESTADO = "distrito-federal"
ALCALDIAS = ["benito-juarez", "cuauhtemoc-2"]
TIPO_PROPIEDAD = "departamento"
TRANSACCION = "for-sale"

# Rangos de precio para evitar el límite de 3000 resultados
RANGOS = [
    {"nombre": "0-3M", "url_params": "?max-price=3000000&priceCurrency=MXN"},
    {"nombre": "3M-6M", "url_params": "?min-price=3000001&max-price=6000000&priceCurrency=MXN"},
    {"nombre": "6M-plus", "url_params": "?min-price=6000001&priceCurrency=MXN"}
]

# Configuración de Google Cloud Storage
USAR_GCS = True  # Cambiar a False para guardar localmente

# CONFIGURACIÓN DE PRUEBA (TESTING)
MAX_PAGINAS_PRUEBA = None  # Definir número de páginas para probar. None para descargar todo.


from scraper_functions import (
    obtener_carpeta_anio_mes,
    construir_url, obtener_nombre_archivo,
    scrape_lamudi, scrape_y_guardar_fallidos,
    guardar_links_fallidos, reintentar_links_fallidos,
    limpiar_df,
    filtrar_por_categoria, contar_propiedades_por_estado_y_tipo
)


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """
    Función principal que coordina el scraping, limpieza y análisis de datos.
    Guarda automáticamente en Google Cloud Storage en la carpeta: Lamudi/YYYY_MM/
    """
    print("\n" + "=" * 80)
    print(f"🏙️  LAMUDI SCRAPER - CDMX: {', '.join(ALCALDIAS).upper()}")
    print(f"🏠 TIPO: {TIPO_PROPIEDAD} | 💰 FILTROS: Rangos de Precio")
    if USAR_GCS:
        carpeta_gcs = obtener_carpeta_anio_mes()
        print(f"☁️  MODO: Google Cloud Storage (scraping_inmuebles/{carpeta_gcs})")
    else:
        print("💾 MODO: Almacenamiento Local")
    
    if MAX_PAGINAS_PRUEBA:
        print(f"🧪 MODO PRUEBA: Limitado a {MAX_PAGINAS_PRUEBA} página(s)")
    
    print("=" * 80 + "\n")
    
    for alcaldia in ALCALDIAS:
        print(f"\n" + "=" * 80)
        print(f"🔍 PROCESANDO ALCALDÍA: {alcaldia.upper()}")
        print(f"🚀 Dividiendo en {len(RANGOS)} rangos de precio para departamentos")
        print("=" * 80)

        stats_descarga = {}

        for rango in RANGOS:
            nombre_rango = rango["nombre"]
            params = rango["url_params"]
            
            print(f"\n📍 Descargando {TIPO_PROPIEDAD.upper()} en rango {nombre_rango}...")
            
            # Construir URL con parámetros de precio
            start_url = f"https://www.lamudi.com.mx/{ESTADO}/{alcaldia}/{TIPO_PROPIEDAD}/{TRANSACCION}/{params}"
            output_filename = f"{alcaldia}_{TIPO_PROPIEDAD}_{nombre_rango}.csv"
            
            try:
                # Descargar datos
                failed = scrape_y_guardar_fallidos(start_url, output_filename, usar_gcs=USAR_GCS, max_paginas=MAX_PAGINAS_PRUEBA)
                
                # Limpiar y guardar
                df_save = limpiar_df(output_filename, usar_gcs=USAR_GCS)
                cleaned_filename = output_filename.replace('.csv', '_clean.csv')
                
                if USAR_GCS:
                    # Guardar en GCS
                    from io import BytesIO
                    from google.cloud import storage
                    from scraper_functions import obtener_cliente_gcs, BUCKET_NAME, obtener_carpeta_anio_mes as get_carpeta
                    
                    carpeta_gcs = get_carpeta()
                    ruta_gcs = f"{carpeta_gcs}{cleaned_filename}"
                    
                    try:
                        cliente = obtener_cliente_gcs()
                        bucket = cliente.bucket(BUCKET_NAME)
                        blob = bucket.blob(ruta_gcs)
                        csv_buffer = BytesIO()
                        df_save.to_csv(csv_buffer, index=False, encoding='utf-8')
                        csv_buffer.seek(0)
                        blob.upload_from_file(csv_buffer, content_type='text/csv')
                        cleaned_archivo_ref = ruta_gcs
                    except Exception as e:
                        print(f"Error guardando archivo limpio en GCS: {e}")
                        cleaned_archivo_ref = cleaned_filename
                else:
                    # Guardar localmente
                    df_save.to_csv(cleaned_filename, index=False)
                    cleaned_archivo_ref = cleaned_filename
                
                stats_descarga[nombre_rango] = {
                    'propiedades': len(df_save),
                    'links_fallidos': len(failed) if failed else 0,
                    'archivo_raw': output_filename,
                    'archivo_clean': cleaned_filename,
                    'archivo_fallidos': f"{alcaldia}_{TIPO_PROPIEDAD}_{nombre_rango}_failed_links.json" if failed else None
                }
                
                print(f"✅ {nombre_rango.upper()}: {len(df_save)} propiedades guardadas")
                if failed:
                    print(f"   ⚠️  {len(failed)} links fallidos (guardados para reintentar)")
                print(f"   📁 Raw: {output_filename}")
                print(f"   📁 Clean: {cleaned_archivo_ref}")
                
            except Exception as e:
                print(f"❌ Error descargando rango {nombre_rango}: {str(e)}")
                stats_descarga[nombre_rango] = {'error': str(e)}

        # Resumen por alcaldía
        print("\n" + "=" * 80)
        print(f"✅ DESCARGA COMPLETA DE {alcaldia.upper()}")
        print("=" * 80)

        total_propiedades = sum(s.get('propiedades', 0) for s in stats_descarga.values())
        total_fallidos = sum(s.get('links_fallidos', 0) for s in stats_descarga.values())

        print(f"\n📊 RESUMEN POR RANGO DE PRECIO ({alcaldia.upper()}):")
        for rango_nombre, stats in stats_descarga.items():
            if 'error' not in stats:
                print(f"   • {rango_nombre:10s}: {stats['propiedades']:>4d} propiedades", end="")
                if stats['links_fallidos'] > 0:
                    print(f" | ⚠️  {stats['links_fallidos']} fallidos")
                else:
                    print()

        print(f"\n📈 TOTALES PARA {alcaldia.upper()}:")
        print(f"   Propiedades descargadas: {total_propiedades}")
        if total_fallidos > 0:
            print(f"   Links fallidos: {total_fallidos}")
            print(f"   📝 Para reintentar consulta el JSON de links fallidos en GCS")
        
        if USAR_GCS:
            carpeta_gcs = obtener_carpeta_anio_mes()
            print(f"\n☁️  UBICACIÓN EN GCS: scraping_inmuebles/{carpeta_gcs}")

    print("\n" + "=" * 80)
    print("🏁 PROCESO FINALIZADO PARA BENITO JUÁREZ Y CUAUHTÉMOC")
    print("=" * 80 + "\n")



if __name__ == "__main__":
    main()
