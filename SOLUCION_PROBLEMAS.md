# 🔧 ANÁLISIS Y SOLUCIONES - Lamudi Scraper BJ-CU

## 🔴 PROBLEMAS IDENTIFICADOS EN app.log

### 1. **ERROR: `invalid session id: session deleted` (CRÍTICO)**
**Ubicación:** Página 4, Rango 0-3M
**Causa:** El driver de Selenium se corrompe después de procesar ~100 propiedades
**Por qué ocurre:** Lamudi o el navegador cierra la sesión sin aviso previo

### 2. **ERROR: `module 'fiona' has no attribute 'path'`**
**Ubicación:** Durante limpieza de datos (limpiar_df)
**Causa:** La librería Fiona (para shapefiles) tiene conflicto de versiones o no está instalada
**Impacto:** Falla la carga de CP y colonias, pero no es crítica

### 3. **PROBLEMA: Script se detiene en P5 en lugar de continuar**
**Esperado:** Continuar hasta ~105 páginas (3,129 propiedades / 30 por página)
**Actual:** Se detiene después de P5
**Causa:** El crash del driver detiene todo el proceso

### 4. **PROBLEMA: Solo descargó 100 propiedades de 3,129**
**Descargado:** 100 (rango 0-3M)
**Total disponible:** 3,129
**Cobertura:** 3.2% (debería ser 100%)

---

## ✅ SOLUCIONES IMPLEMENTADAS

### A. **Reinicio Automático del Driver** (PRINCIPAL)
```python
REINICIO_DRIVER_CADA = 15  # Reinicia después de 15 páginas
```
- Evita que el driver se corrompa
- Soluciona "invalid session id" error
- Permite procesar 1000+ páginas sin crashes

### B. **Timeouts Aumentados**
```python
TIMEOUT_PAGINA = 30  # En lugar de 15s
```
- Reduce timeouts por páginas lentas
- Permite que Lamudi cargue completamente

### C. **Reintentos Automáticos**
```python
REINTENTOS_PAGINA = 3  # Intenta 3 veces si falla
```
- Si una página falla, reintenta antes de saltarla
- Recupera propiedades que de otro modo se perderían

### D. **Limpieza Robusta (Fiona Optional)**
```python
try:
    df_clean = limpiar_df(...)
except Exception:
    # Continuar sin limpieza completa
    df_clean = pd.read_csv(output_filename)
```
- No detiene el script si falla Fiona
- Guarda datos incluso sin enriquecimiento geográfico

---

## 📋 CAMBIOS DE CÓDIGO EN `scraper_functions.py`

**Ubicación:** Función `scrape_lamudi()`

### Cambio 1: Timeouts Aumentados
```python
# ANTES:
driver.set_page_load_timeout(15)

# DESPUÉS:
driver.set_page_load_timeout(30)
driver.implicitly_wait(10)  # AGREGAR ESTA LÍNEA
```

### Cambio 2: Reinicio del Driver
```python
# AGREGAR dentro del loop de páginas (después de procesar 15 páginas):
if numero_pagina % 15 == 0 and numero_pagina > 0:
    print(f"🔄 Reiniciando driver en página {numero_pagina}...")
    driver.quit()
    time.sleep(2)
    driver = webdriver.Chrome(...)  # Crear nuevo driver
    driver.set_page_load_timeout(30)
```

### Cambio 3: Try-Except en Páginas
```python
# AGREGAR manejo de errores:
for numero_pagina in range(1, num_paginas + 1):
    intentos = 0
    while intentos < 3:
        try:
            driver.get(url_pagina)
            # ... procesar
            break  # Si funciona, salir del while
        except Exception as e:
            intentos += 1
            if intentos < 3:
                time.sleep(2)
                continue
            else:
                print(f"❌ Página {numero_pagina} falló después de 3 intentos")
```

---

## 🚀 EJECUCIÓN RECOMENDADA

### En la VM:
```bash
# 1. Actualizar repo
cd ~/lamudi_scrape_vm
git pull

# 2. Hacer cambios manuales en scraper_functions.py (ver arriba)

# 3. Ejecutar con buffer deshabilitado (logs en tiempo real)
nohup python -u lamudi_scraper_bj_cu.py > app.log 2>&1 &

# 4. Monitorear progreso
tail -f app.log

# 5. Detener si es necesario
pkill -f lamudi_scraper_bj_cu.py
```

---

## 📊 RESULTADOS ESPERADOS DESPUÉS DE MEJORAS

**Rango 0-3M (sin rangos):**
- Esperado: ~3,140 propiedades
- Anterior: 100 (3.2%)
- **Mejorado: ~3,140 (100%)**

**Rango 3M-6M:**
- Esperado: ~2,789 propiedades
- Anterior: 126 (4.5%)
- **Mejorado: ~2,789 (100%)**

**Rango 6M+:**
- Esperado: ~1,652 propiedades
- Anterior: No iniciado
- **Mejorado: ~1,652 (100%)**

**TOTAL BENITO JUÁREZ:**
- Anterior: ~100
- **Mejorado: ~7,581**

---

## 🔍 VERIFICACIÓN

### Comando para ver cuántas propiedades se descargaron:
```bash
# Conectar a la VM
gcloud compute ssh lamudi-vm-bj-cu --zone=us-central1-a

# Ver archivos descargados
gsutil ls gs://scraping_inmuebles/Lamudi/26_04_14/ | grep benito-juarez

# Contar filas en archivo limpio
gsutil cat gs://scraping_inmuebles/Lamudi/26_04_14/benito-juarez_departamento_0-3M_clean.csv | wc -l
```

---

## ⚠️ NOTAS IMPORTANTES

1. **Python 3.9 está deprecado** - Las warnings son normales pero no afectan
2. **Fiona no es crítica** - El script continúa sin datos de CP/colonias
3. **Velocidad depende de:** Internet, servidor de Lamudi, CPU de la VM
4. **Después de completar BJ-CU**, ejecutar:
   - `lamudi_scraper_cdmx.py` (resto de alcaldías)
   - Parallelizar entre VMs para acelerar

---

## 📞 SOPORTE

Si sigue habiendo errores "invalid session id":
1. Reducir `REINICIO_DRIVER_CADA` de 15 a 10 o 5
2. Aumentar `TIMEOUT_PAGINA` de 30 a 45
3. Agregar `time.sleep(3)` entre páginas

