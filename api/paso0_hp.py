import os
import time
import pandas as pd
import glob
import shutil  # Necesario para mover entre carpetas en paralelo
import math    # Necesario para dividir los lotes
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor # Para paralelismo

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from dotenv import load_dotenv
from logger import get_logger

# Configuración
logger = get_logger("paso0_hp", log_dir="logs", log_file="paso0.log")
load_dotenv()

OUTPUT_FOLDER = os.path.abspath("./input_pdfs")
URL_LOGIN = os.getenv("LOGIN_URL")
URL_ANTECEDENTES = os.getenv("URL_ANTECEDENTES")
EMAIL_HP = os.getenv("USUARIO_HP")
PASS_HP = os.getenv("PASSWORD_HP")

OPTIMIZAR_CARGA = True 
MANTENER_NAVEGADOR = False
WORKERS = 2 

if not URL_ANTECEDENTES or not URL_LOGIN:
    logger.error("❌ ERROR CRÍTICO: No se cargaron las URL del archivo .env")
    logger.error(f"   URL_ANTECEDENTES: {URL_ANTECEDENTES}")
    logger.error(f"   URL_LOGIN: {URL_LOGIN}")
    raise ValueError("Faltan variables de entorno en el archivo .env")
# ==============================================================================
# 1. CARGA DE DATOS (IGUAL AL ORIGINAL)
# ==============================================================================
def detectar_y_cargar(ruta_archivo: str) -> Optional[List[Dict[str, Any]]]:
    logger.info(f"📂 Intentando cargar archivo origen: {ruta_archivo}")
    if not os.path.exists(ruta_archivo):
        logger.error(f"❌ El archivo no existe: {ruta_archivo}")
        return None
    try:
        if ruta_archivo.endswith('.csv'):
            df = pd.read_csv(ruta_archivo, sep=';', encoding='utf-8')
            logger.debug("   📄 Formato detectado: CSV")
        elif ruta_archivo.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(ruta_archivo)
            logger.debug("   📄 Formato detectado: EXCEL")
        else:
            logger.error("❌ Formato no soportado (solo .csv, .xlsx, .xls)")
            return None
        
        df.columns = [c.strip().lower() for c in df.columns]
        logger.info(f"   ✅ Archivo cargado. Filas encontradas: {len(df)}")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"❌ Error leyendo archivo: {e}", exc_info=True)
        return None

def estandarizar_data(lista_cruda: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.debug("🧹 Estandarizando y limpiando datos de entrada...")
    lista_limpia = []
    for item in lista_cruda:
        r = str(item.get("rol", "")).strip()
        c = str(item.get("comuna", "")).strip()
        if len(r) > 3 and len(c) > 3:
            lista_limpia.append({"rol": r, "comuna": c})
        else:
            logger.warning(f"   ⚠️ Fila descartada por datos incompletos: Rol='{r}', Comuna='{c}'")
            
    logger.info(f"✅ Datos listos para procesar: {len(lista_limpia)} propiedades.")
    return lista_limpia

# ==============================================================================
# 2. CONFIGURACIÓN SELENIUM (SOLO AGREGADO PARAMETRO DE CARPETA)
# ==============================================================================
def _configurar_driver(carpeta_descarga=None):
    target = carpeta_descarga if carpeta_descarga else OUTPUT_FOLDER
    
    logger.info("🔧 Configurando Driver Chrome (Opciones y Preferencias - Modo Ahorro)...")
    options = Options()
    
    # Optimización de recursos
    options.add_argument("--headless=new") 
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    options.page_load_strategy = 'eager'
    
    logger.debug(f"   📂 Carpeta de descargas configurada: {target}")
    prefs = {
        "download.default_directory": target,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.managed_default_content_settings.images": 2, 
        "profile.default_content_setting_values.notifications": 2, 
    }

    logger.debug(f"informacion de configuracion del driver: {options}")

    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=options)

def _esperar_descarga(carpeta, timeout=60):
    logger.debug(f"   ⏳ Monitoreando carpeta {carpeta} (Timeout: {timeout}s)...")
    fin = time.time() + timeout
    while time.time() < fin:
        if glob.glob(os.path.join(carpeta, "*.crdownload")):
            time.sleep(0.5)
            continue
        archivos = glob.glob(os.path.join(carpeta, "*.pdf"))
        if archivos:
            ultimo_archivo = max(archivos, key=os.path.getctime)
            try:
                if time.time() - os.path.getctime(ultimo_archivo) < timeout and os.path.getsize(ultimo_archivo) > 0:
                    logger.debug(f"   📄 Archivo detectado: {os.path.basename(ultimo_archivo)}")
                    return str(ultimo_archivo)
            except:
                pass
        time.sleep(1)
    logger.warning("   ⚠️ No se detectó archivo nuevo tras el tiempo de espera.")
    return None

def _iniciar_sesion_hp(driver, wait):
    logger.info("🔐 Iniciando proceso de Login...")
    driver.get(URL_LOGIN)
    try:
        logger.debug("   ⌨️ Ingresando credenciales...")
        wait.until(EC.element_to_be_clickable((By.ID, "id_email"))).send_keys(EMAIL_HP)
        driver.find_element(By.ID, "id_password").send_keys(PASS_HP)
        driver.find_element(By.ID, "hp-login-btn").click()
        
        logger.debug("   ⏳ Esperando redirección post-login...")
        wait.until(lambda d: "/login" not in d.current_url)
        logger.success("✅ Login exitoso. Sesión iniciada.")
        return True
    except Exception as e:
        logger.error(f"❌ Error en Login: {e}", exc_info=True)
        try:
            timestamp = int(time.time())
            # 1. Tomar FOTO de lo que ve el bot
            screenshot_path = f"debug_login_fail_{timestamp}.png"
            driver.save_screenshot(screenshot_path)
            logger.warning(f"📸 Screenshot del error guardado en: {screenshot_path}")
            
            # 2. Guardar HTML para ver si hay mensajes de error ocultos
            with open(f"debug_login_fail_{timestamp}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.warning(f"📄 HTML del error guardado.")
            
            # 3. Intentar leer mensaje de error en pantalla
            if "credenciales" in driver.page_source.lower() or "incorrecto" in driver.page_source.lower():
                logger.error("🕵️ DETECTADO: El sitio dice que las credenciales son incorrectas.")
        except:
            pass
        return False

# ==============================================================================
# 3. LÓGICA DE DESCARGA (SOLO MODIFICADO EL GUARDADO FINAL)
# ==============================================================================
def _descargar_pdf_individual(driver, wait, item, carpeta_temporal):
    rol = item['rol']
    comuna = item['comuna']
    
    logger.info(f"🔎 Procesando descarga: {comuna} - Rol {rol}")

    try:
        # Refrescar estado
        if driver.current_url != URL_ANTECEDENTES:
            driver.get(URL_ANTECEDENTES)
        else:
            driver.refresh()
        
        wait.until(EC.presence_of_element_located((By.ID, "select-comuna")))

        # A) COMUNA
        select_comuna = driver.find_element(By.ID, "select-comuna")
        driver.execute_script("arguments[0].style.display = 'block';", select_comuna)
        try:
            Select(select_comuna).select_by_visible_text(comuna)
            logger.debug(f"   🏙️ Seleccionando comuna '{comuna}'...")
        except:
            Select(select_comuna).select_by_visible_text(comuna.title())
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", select_comuna)
        

        # B) ROL
        input_rol = driver.find_element(By.ID, "rol")
        logger.debug(f" buscando elemento por ID")
        input_rol.clear()
        logger.debug(f" limpiando campo:")
        input_rol.send_keys(rol)
        logger.debug(f" ingresando rol: {rol}")

        # C) BUSCAR
        btn_buscar = driver.find_element(By.ID, "btn-search-rol")
        driver.execute_script("arguments[0].click();", btn_buscar)
        logger.debug(f" CLICK BUSCAR")

        # PASO 5: Esperar botón Generar
        logger.debug("   ⏳ Esperando confirmación de propiedad (Botón Generar)...")
        btn_generar = wait.until(EC.visibility_of_element_located((By.ID, "btn-submit")))
        
        # PASO 6: Click Generar
        driver.execute_script("arguments[0].click();", btn_generar)
        logger.debug(f" CLICK GENERAR")

        # --- MODIFICACIÓN 1: TIMEOUT AUMENTADO A 150 SEGUNDOS ---
        logger.debug("   ⏳ Esperando que el servidor genere el PDF (Timeout: 150s)...")
        xpath_download = "//a[contains(., 'Descargar PDF')]"
        
        # Aumentamos tolerancia para servidores lentos
        wait_descarga = WebDriverWait(driver, 150) 
        
        btn_descarga = wait_descarga.until(EC.element_to_be_clickable((By.XPATH, xpath_download)))
        logger.debug(f" CLICK DESCARGAR")        
        # Limpieza previa
        for f in glob.glob(os.path.join(carpeta_temporal, "*")):
            try: os.remove(f)
            except: pass

        # PASO 9: Descargar
        logger.info(f"   📥 Informe listo. Click en Descargar PDF...")
        driver.execute_script("arguments[0].click();", btn_descarga)
        
        archivo = _esperar_descarga(carpeta_temporal)
        
        if archivo:
            nombre_final = os.path.join(OUTPUT_FOLDER, f"{comuna}_{rol}.pdf".replace(" ", "_").replace("/", "-"))
            if os.path.exists(nombre_final):
                try: os.remove(nombre_final)
                except: pass
            
            logger.debug(f" MOVIMIENTO ARCHIVO") # Reintentos de movimiento de archivo
            movido = False
            for intento in range(5):
                logger.debug(f" Reintentos en mover el archivo: {intento + 1}")
                try:
                    shutil.move(archivo, nombre_final)
                    movido = True
                    break
                except PermissionError:
                    time.sleep(1)
                except Exception:
                    time.sleep(1)
            
            if movido:
                logger.success(f"   ✅ Descarga completada: {os.path.basename(nombre_final)}")
                return True
            else:
                logger.error(f"   ❌ No se pudo mover el archivo final: {nombre_final}")
                return False
        else:
            logger.error("   ❌ Timeout esperando la descarga física del archivo.")
            return False

    except Exception as e:
        logger.error(f"   ❌ Fallo durante la descarga: {e}")
        return False

# ==============================================================================
# NUEVO: WORKER (NUEVA FUNCIÓN, NO EXISTÍA, NECESARIA PARA THREADS)
# ==============================================================================
def procesar_lote_worker(id_worker, sublista_propiedades, cancel_event):
    carpeta_worker = os.path.join(OUTPUT_FOLDER, f"temp_{id_worker}")
    if not os.path.exists(carpeta_worker):
        os.makedirs(carpeta_worker)

    driver = _configurar_driver(carpeta_worker)
    wait = WebDriverWait(driver, 15) # Wait corto para elementos UI normales
    
    exitos = 0
    fallidos = [] # Lista local de fallos del worker

    try:
        if not _iniciar_sesion_hp(driver, wait):
            logger.error(f"      💀 [Worker-{id_worker}] Fallo definitivo")
            return 0, sublista_propiedades # Retorna 0 éxitos y toda la lista como fallida
        
        for item in sublista_propiedades:
            if cancel_event.is_set(): break
            
            # --- MODIFICACIÓN 2: BUCLE DE REINTENTOS (3 Intentos) ---
            exito_item = False
            max_intentos = 3
            logger.debug(f"      🔄 [Worker-{id_worker}] reintentando descarga para: {item['rol']}")
            for intento in range(max_intentos):
                if cancel_event.is_set(): break
                
                if intento > 0:
                    logger.warning(f"      🔄 [Worker-{id_worker}] Reintentando ({intento+1}/{max_intentos})...")
                    time.sleep(3) # Pausa para respirar
                
                if _descargar_pdf_individual(driver, wait, item, carpeta_worker):
                    exito_item = True
                    logger.success(f"      ✅ [Worker-{id_worker}] Éxito para: {item['rol']}")
                    exitos += 1
                    break # Salir del loop de intentos si funcionó
            
            if not exito_item:
                logger.error(f"      💀 [Worker-{id_worker}] Fallo definitivo para: {item['rol']}")
                fallidos.append(item)
            # --------------------------------------------------------
            
    finally:
        driver.quit()
        try: shutil.rmtree(carpeta_worker)
        except: pass
    
    # Retornamos TUPLA: (cantidad_exitos, lista_de_dict_fallidos)
    return exitos, fallidos

# ==============================================================================
# ORQUESTADOR (ADAPTADO A THREADPOOLEXECUTOR)
# ==============================================================================
def orquestador_descargas(lista_propiedades, cancel_event, callback_progreso=None):
    if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)
    
    total = len(lista_propiedades)
    logger.info(f"🚀 Iniciando ciclo de descargas PARALELO para {total} propiedades. WORKERS={WORKERS}")

    # Dividimos la lista en partes iguales para los workers
    chunk_size = math.ceil(total / WORKERS)
    chunks = [lista_propiedades[i:i + chunk_size] for i in range(0, total, chunk_size)]
    
    total_exitos = 0
    total_fallidos = []
    
    # Nuevo: Contador para progreso
    procesados_global = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            # Lanzamos cada worker con su ID y su pedazo de lista
            futures.append(executor.submit(procesar_lote_worker, i+1, chunk, cancel_event))
        
        # Esperamos resultados
        for future in futures:
            try:
                # Desempaquetamos la tupla de retorno
                e_count, f_list = future.result()
                total_exitos += e_count
                total_fallidos.extend(f_list)
                
                # --- NUEVO: Actualización de progreso ---
                # Sumamos los éxitos + fallidos de este chunk para saber cuántos se procesaron
                procesados_global += (e_count + len(f_list))
                if callback_progreso:
                    callback_progreso(procesados_global, total)
                # ----------------------------------------

            except Exception as e:
                logger.error(f"❌ Error crítico en worker: {e}")

    logger.info("="*60)
    logger.info(f"📊 REPORTE FINAL DE DESCARGAS")
    logger.info(f"   - Total Intentados: {total}")
    logger.info(f"   - Descargados OK:   {total_exitos}")
    logger.info(f"   - Fallidos:         {len(total_fallidos)}")
    logger.info("="*60)
    
    if total_fallidos:
        logger.warning("   ⚠️ Lista de Propiedades Fallidas (No estarán en el Excel):")
        for f in total_fallidos:
            logger.warning(f"      ❌ Rol: {f.get('rol')} | Comuna: {f.get('comuna')}")
    logger.info("="*60)

    logger.success(f"🏁 Proceso finalizado. Descargas exitosas: {total_exitos}/{total}")     
    
    return total_exitos > 0

# ==============================================================================
# ENTRY POINT
# ==============================================================================
# Modificación: Agregar callback_progreso=None
def ejecutar(ruta_archivo: str, cancel_event, callback_progreso=None) -> bool:
    logger.info(f"=== PASO 0: INICIO DEL FLUJO DE DESCARGAS ===")
    logger.info(f"📂 Archivo de entrada: {ruta_archivo}")
    
    raw = detectar_y_cargar(ruta_archivo)
    if not raw: 
        logger.error("❌ No se pudieron cargar los datos iniciales. Abortando.")
        return False
        
    clean = estandarizar_data(raw)
    if not clean: 
        logger.error("❌ No hay datos válidos después de la limpieza. Abortando.")
        return False
        
    # Pasamos el callback al orquestador
    return orquestador_descargas(clean, cancel_event, callback_progreso=callback_progreso)