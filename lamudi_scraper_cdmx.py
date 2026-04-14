"""
Lamudi Web Scraper - CDMX (Resto de Alcaldías)
Descarga propiedades para todas las alcaldías de CDMX exceptuando 
Benito Juárez y Cuauhtémoc-2 (que se manejan por separado para departamentos).
"""

import pandas as pd
from datetime import datetime
from io import BytesIO
from scraper_functions import (
    obtener_carpeta_anio_mes,
    construir_url, obtener_nombre_archivo,
    scrape_lamudi, scrape_y_guardar_fallidos,
    guardar_links_fallidos, reintentar_links_fallidos,
    limpiar_df,
    filtrar_por_categoria, contar_propiedades_por_estado_y_tipo,
    obtener_cliente_gcs, BUCKET_NAME
)

# ============================================================================
# CONFIGURACIÓN ESPECÍFICA CDMX (GENERAL)
# ============================================================================

ESTADO = "distrito-federal"

# 🔥 TODAS las alcaldías de CDMX
ALCALDIAS = [
    "alvaro-obregon",
    "azcapotzalco",
    "benito-juarez",           # Se ignorará para departamentos somente
    "coyoacan",
    "cuajimalpa-de-morelos",
    "cuauhtemoc-2",            # Se ignorará para departamentos somente
    "gustavo-a-madero",
    "iztacalco",
    "iztapalapa",
    "la-magdalena-contreras",
    "miguel-hidalgo",
    "milpa-alta",
    "tlahuac",
    "tlalpan",
    "venustiano-carranza-1",
    "xochimilco"
]

TIPOS_PROPIEDAD = ["casa", "departamento", "terreno", "comercial", "offices"]
TRANSACCION = "for-sale"

# Configuración de Google Cloud Storage
USAR_GCS = True  # Cambiar a False para guardar localmente

# CONFIGURACIÓN DE PRUEBA (TESTING)
MAX_PAGINAS_PRUEBA = None  # Definir número de páginas para probar. None para descargar todo.


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """
    Función principal que coordina el scraping, limpieza y análisis de datos.
    Guarda automáticamente en Google Cloud Storage en la carpeta: Lamudi/YYYY_MM/
    """
    print("\n" + "=" * 80)
    print(f"🏙️  LAMUDI SCRAPER - CDMX (ALCALDÍAS GENERAL)")
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
        print(f"📍 PROCESANDO ALCALDÍA: {alcaldia.upper()}")
        print(f"🔍 Descargando tipos de propiedades para CDMX")
        print("=" * 80)

        stats_descarga = {}

        for tipo in TIPOS_PROPIEDAD:
            # 🛑 REGLA ESPECIAL: Ignorar departamentos en BJ y Cuauhtémoc (se hacen en otro script con rangos)
            if tipo == "departamento" and alcaldia in ["benito-juarez", "cuauhtemoc-2"]:
                print(f"⏩ Saltando {tipo.upper()} en {alcaldia} (ignorado por solicitud)")
                continue

            print(f"\n📍 Descargando {tipo.upper()}...")
            
            start_url = f"https://www.lamudi.com.mx/{ESTADO}/{alcaldia}/{tipo}/{TRANSACCION}/"
            output_filename = f"{alcaldia}_{tipo}.csv"
            
            try:
                # Descargar datos
                failed = scrape_y_guardar_fallidos(start_url, output_filename, usar_gcs=USAR_GCS, max_paginas=MAX_PAGINAS_PRUEBA)
                
                # Limpiar y guardar
                df_save = limpiar_df(output_filename, usar_gcs=USAR_GCS)
                cleaned_filename = output_filename.replace(".csv", "_clean.csv")
                
                if USAR_GCS:
                    # Guardar en GCS
                    from scraper_functions import obtener_carpeta_anio_mes as get_carpeta
                    
                    carpeta_gcs = get_carpeta()
                    ruta_gcs = f"{carpeta_gcs}{cleaned_filename}"
                    
                    try:
                        cliente = obtener_cliente_gcs()
                        bucket = cliente.bucket(BUCKET_NAME)
                        blob = bucket.blob(ruta_gcs)
                        csv_buffer = BytesIO()
                        df_save.to_csv(csv_buffer, index=False, encoding="utf-8")
                        csv_buffer.seek(0)
                        blob.upload_from_file(csv_buffer, content_type="text/csv")
                        cleaned_archivo_ref = ruta_gcs
                    except Exception as e:
                        print(f"Error guardando archivo limpio en GCS: {e}")
                        cleaned_archivo_ref = cleaned_filename
                else:
                    # Guardar localmente
                    df_save.to_csv(cleaned_filename, index=False)
                    cleaned_archivo_ref = cleaned_filename
                
                stats_descarga[tipo] = {
                    "propiedades": len(df_save),
                    "links_fallidos": len(failed) if failed else 0,
                    "archivo_raw": output_filename,
                    "archivo_clean": cleaned_filename,
                    "archivo_fallidos": f"{alcaldia}_{tipo}_failed_links.json" if failed else None
                }
                
                print(f"✅ {tipo.upper()}: {len(df_save)} propiedades guardadas")
                if failed:
                    print(f"   ⚠️  {len(failed)} links fallidos (guardados para reintentar)")
                print(f"   📁 Raw: {output_filename}")
                print(f"   📁 Clean: {cleaned_archivo_ref}")
                
            except Exception as e:
                print(f"❌ Error descargando {tipo}: {str(e)}")
                stats_descarga[tipo] = {"error": str(e)}

        # Resumen por alcaldía
        print("\n" + "=" * 80)
        print(f"✅ DESCARGA COMPLETA DE {alcaldia.upper()}")
        print("=" * 80)

        total_propiedades = sum(s.get("propiedades", 0) for s in stats_descarga.values())
        total_fallidos = sum(s.get("links_fallidos", 0) for s in stats_descarga.values())

        print(f"\n📊 RESUMEN POR TIPO ({alcaldia.upper()}):")
        for tipo, stats in stats_descarga.items():
            if "error" not in stats:
                print(f"   • {tipo:15s}: {stats['propiedades']:>4d} propiedades", end="")
                if stats["links_fallidos"] > 0:
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
    print("🏁 PROCESO FINALIZADO PARA ALCALDÍAS DE CDMX")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
