import os
import time
import pandas as pd
import glob
import shutil
import math
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from dotenv import load_dotenv
from logger import get_logger

# # Configuración
logger = get_logger("paso0_hp", log_dir="logs", log_file="paso0.log")
load_dotenv()

OUTPUT_FOLDER = os.path.abspath("./input_pdfs")
URL_LOGIN = os.getenv("LOGIN_URL")
URL_ANTECEDENTES = os.getenv("URL_ANTECEDENTES")
URL_TASACIONES = os.getenv("URL_TASACIONES")
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
        
        # Desactivar visor de imágenes y notificaciones (Optimización)
        "profile.managed_default_content_settings.images": 2, 
        "profile.default_content_setting_values.notifications": 2, 
        
        # --- NUEVO: Desactivar Escaneo SafeBrowsing (EL BLINDAJE) ---
        "safebrowsing.enabled": False,  # Mantenemos el servicio activo...
        "safebrowsing.disable_download_protection": True, # ...pero desactivamos protección de descargas
        "profile.default_content_settings.popups": 0,
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
        
        # Forzar que NO pregunte por confirmación en descargas peligrosas
        "download.prompt_for_download": False,
        "safebrowsing.disable_extension_blacklist": True,
    }

    logger.debug(f"informacion de configuracion del driver: {options}")

    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=options)

def _reparar_descargas_bloqueadas(driver):
    """
    Entra al gestor de descargas de Chrome, busca items bloqueados/peligrosos
    y fuerza la aprobación (Click en 'Conservar' / 'Keep').
    """
    logger.info("   🚑 Iniciando protocolo de reparación de descargas en Chrome...")
    reparado = False
    try:
        # Abrir pestaña de descargas
        driver.execute_script("window.open('chrome://downloads/', '_blank');")
        time.sleep(1) # Esperar apertura
        driver.switch_to.window(driver.window_handles[-1])
        
        # Script JS para penetrar Shadow DOM y hacer click en "Conservar"
        # Busca botones de acción en items peligrosos
        js_fix = """
            const manager = document.querySelector('downloads-manager');
            if (manager && manager.shadowRoot) {
                const items = manager.shadowRoot.querySelectorAll('downloads-item');
                if (items.length > 0) {
                    const item = items[0]; // Miramos el último archivo (el de arriba)
                    
                    // Si está en estado peligroso o advertencia
                    if (item.state === 'DANGEROUS' || item.state === 'IN_PROGRESS') {
                        
                        // Intento 1: Botón "Conservar" (Save) en el Shadow DOM
                        const saveBtn = item.shadowRoot.querySelector('cr-button[focus-type="save"]');
                        if (saveBtn) {
                            saveBtn.click();
                            return "FIXED_SAVE";
                        }
                        
                        // Intento 2: Botón de acción genérica "Conservar" (Dangerous)
                        const dangerousBtn = item.shadowRoot.querySelector('#dangerous .action-button');
                        if (dangerousBtn) {
                            dangerousBtn.click();
                            return "FIXED_DANGEROUS";
                        }
                        
                        // Intento 3: Enlace "Conservar archivo peligroso" (versiones antiguas)
                        const linkKeep = item.shadowRoot.querySelector('a#safe');
                        if (linkKeep) {
                            linkKeep.click();
                            return "FIXED_LINK";
                        }
                        
                        return "FOUND_BUT_NO_BUTTON";
                    }
                }
            }
            return "NO_ISSUE_FOUND";
        """
        
        # Ejecutamos el fix
        resultado = driver.execute_script(js_fix)
        logger.debug(f"   🔧 Resultado script reparación: {resultado}")
        
        if "FIXED" in resultado:
            reparado = True
            time.sleep(2) # Dar tiempo a Chrome para finalizar la escritura en disco
            
    except Exception as e:
        logger.warning(f"   ⚠️ Falló el intento de reparación manual: {e}")
    finally:
        # Cerrar pestaña de descargas y volver a la principal
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            
    return reparado


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
    import json 
    
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
        rol = "-".join([p.lstrip("0") or "0" for p in str(rol).replace("−", "-").replace("–", "-").replace("—", "-").split("-")])
        input_rol.send_keys(rol)
        logger.debug(f" ingresando rol: {rol}")

        # C) BUSCAR
        btn_buscar = driver.find_element(By.ID, "btn-search-rol")
        driver.execute_script("arguments[0].click();", btn_buscar)
        logger.debug(f" CLICK BUSCAR")

        try:
            logger.debug("   ⏳ Validando existencia del rol...")
            condicion = EC.any_of(
                EC.visibility_of_element_located((By.ID, "btn-submit")),
                EC.visibility_of_element_located((By.ID, "search-rol-response-container"))
            )
            # 10 segundos es suficiente para que el servidor responda si el rol existe o no
            elemento_detectado = WebDriverWait(driver, 10).until(condicion)
            
            if elemento_detectado.get_attribute("id") == "search-rol-response-container":
                error_texto = driver.find_element(By.ID, "search-rol-response").text
                logger.error(f"   ❌ ERROR DEL SITIO: '{error_texto}' para Rol {rol} en {comuna}")
                return "ROL_NOT_FOUND"
                
        except TimeoutException:
            logger.warning("   ⚠️ El sitio no respondió a la búsqueda (Timeout validación).")
            try:
                timestamp = int(time.time())
                screenshot_path = f"debug_timeout_{comuna}_{rol}_{timestamp}.png".replace(" ", "_").replace("/", "-")
                driver.save_screenshot(screenshot_path)
                logger.warning(f"   📸 Screenshot del error guardado en: {screenshot_path}")
            except Exception as e_foto:
                logger.error(f"   ❌ No se pudo tomar screenshot del timeout: {e_foto}")
            return False

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
        
        # --- CAPTURA DEL LINK ---
        link_informe = btn_descarga.get_attribute("href")
        logger.debug(f"   🔗 Link detectado: {link_informe}")
        # -------------------------------

        logger.debug(f" CLICK DESCARGAR")        
        # Limpieza previa
        for f in glob.glob(os.path.join(carpeta_temporal, "*")):
            try: os.remove(f)
            except: pass

        # PASO 9: Descargar
        logger.info(f"   📥 Informe listo. Click en Descargar PDF...")
        driver.execute_script("arguments[0].click();", btn_descarga)
        
        archivo = _esperar_descarga(carpeta_temporal, timeout=30)
        
        if not archivo:
            logger.warning("   ⚠️ Descarga no finalizada. Verificando bloqueos de Chrome...")
            se_reparo = _reparar_descargas_bloqueadas(driver)
            
            if se_reparo:
                logger.info("   ✅ Bloqueo reparado. Esperando archivo final...")
                # 3. Segundo intento de espera (damos 15s extra para que termine de escribir)
                archivo = _esperar_descarga(carpeta_temporal, timeout=15)

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

                    datos_tasacion = _extraer_tasaciones(driver, wait, rol, comuna)

                    print(datos_tasacion)
                    # Guardamos un pequeño JSON con el mismo nombre pero extensión .json
                    ruta_meta = nombre_final + ".json"
                    meta_data = {
                        "link_informe": link_informe,
                        "rol_origen": rol,        # <--- Para validar luego
                        "comuna_origen": comuna,
                        **datos_tasacion
                    }
                    with open(ruta_meta, "w", encoding="utf-8") as f:
                        json.dump(meta_data, f)

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
            logger.error("   ❌ Timeout definitivo: El archivo no se descargó (incluso tras intento de reparación).")
            return False

    except Exception as e:
        logger.error(f"   ❌ Fallo durante la descarga: {e}")
        return False

    except Exception as e:
        logger.error(f"   ❌ Fallo durante la descarga: {e}")
        return False

# ==============================================================================
# 4. Extraer tasaciones (FASE 2: NAVEGACIÓN WIZARD)
# ==============================================================================
def _extraer_tasaciones(driver, wait, rol, comuna):
    """
    Navega al tasador, busca la propiedad y avanza por el wizard (Next -> Next -> Ver Tasación).
    Incluye lógica de reintento si aparece una alerta de error del sitio o timeout del servidor.
    """
    # IMPORT CRÍTICO: Aseguramos que todas las excepciones estén disponibles en este scope
    from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException, TimeoutException
    
    logger.info(f"   💰 Iniciando búsqueda en Tasador para {rol} ({comuna})...")
    
    # Estructura base
    data = {
        "tasa_vta_clp": 0,
        "tasa_vta_uf": "0", 
        "tasa_arr_clp": 0,
        "tasa_arr_uf": "0" 
    }

    MAX_INTENTOS = 3

    for intento in range(1, MAX_INTENTOS + 1):
        try:
            # 1. Navegación (Siempre reiniciamos la navegación en cada intento)
            driver.get(URL_TASACIONES)
            
            # 2. Seleccionar Tab "Rol"
            try:
                xpath_toggle = "//label[.//input[@value='search-rol']]"
                toggle_rol = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_toggle)))
                toggle_rol.click()
            except TimeoutException:
                # Fallback por si el toggle no responde rápido
                driver.find_element(By.XPATH, "//label[contains(., 'Rol')]").click()

            # 3. Seleccionar Comuna (Lógica Inyección JS)
            try:
                select_comuna = driver.find_element(By.ID, "select-comuna")
                driver.execute_script("arguments[0].style.display = 'block';", select_comuna)
                try:
                    Select(select_comuna).select_by_visible_text(comuna)
                except:
                    Select(select_comuna).select_by_visible_text(comuna.title())
                driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", select_comuna)
            except Exception as e:
                logger.warning(f"   ⚠️ Error seleccionando comuna en Tasador: {e}")

            # 4. Ingresar Rol
            input_rol = wait.until(EC.visibility_of_element_located((By.ID, "rol")))
            input_rol.clear()
            input_rol.send_keys(rol)

            # 5. Click Buscar
            btn_buscar = driver.find_element(By.ID, "btn-search-rol")
            driver.execute_script("arguments[0].click();", btn_buscar)
            logger.debug(f"   🖱️ [Intento {intento}] Click en Buscar Tasación...")

            # 6. Espera Inteligente: ¿Éxito o Fallo?
            locator_exito = (By.ID, "form-summary")
            locator_error = (By.ID, "search-rol-response")

            elemento_resultado = wait.until(EC.any_of(
                EC.visibility_of_element_located(locator_exito),
                EC.visibility_of_element_located(locator_error)
            ))

            # 7. Validación y Navegación del Wizard
            id_resultado = elemento_resultado.get_attribute("id")
            
            if id_resultado == "form-summary":
                logger.success(f"   ✅ Tasador: Propiedad encontrada. Iniciando secuencia de Wizard...")
                
                # --- FASE 2: SECUENCIA DE BOTONES ---
                
                # Paso A: Click Siguiente 1 (Características)
                btn_next_1 = wait.until(EC.element_to_be_clickable((By.ID, "btn-next-1")))
                driver.execute_script("arguments[0].click();", btn_next_1)
                
                # Paso B: Click Siguiente 2 (Mapa/Ubicación)
                btn_next_2 = wait.until(EC.element_to_be_clickable((By.ID, "btn-next-2")))
                driver.execute_script("arguments[0].click();", btn_next_2)
                
                # Paso C: Click Ver Tasación (Final)
                btn_next_3 = wait.until(EC.element_to_be_clickable((By.ID, "btn-next-3")))
                driver.execute_script("arguments[0].click();", btn_next_3)
                logger.debug("   🚀 Wizard: Paso 3 completado (Ver Tasación). Esperando carga final...")
                
                # --- FASE 3 (Extracción de precios final) ---
                try:
                    # 1. Esperamos explícitamente a que el botón "Ver Tasación" DESAPAREZCA.
                    # Esto confirma que el click funcionó y la UI está transicionando.
                    wait.until(EC.invisibility_of_element_located((By.ID, "btn-next-3")))
                    
                    # 2. Definimos un Wait ESPECIAL de 60 segundos para la tasación (el global suele ser corto)
                    wait_tasacion = WebDriverWait(driver, 150)
                    
                    # 3. Esperamos a que aparezca el elemento de precio
                    xpath_precio_venta = "//h3[contains(., 'Precio estimado de venta')]/following::span[contains(@class, 'text-4xl')]"
                    wait_tasacion.until(EC.visibility_of_element_located((By.XPATH, xpath_precio_venta)))
                    
                except TimeoutException:
                    logger.warning(f"   ⏳ [Intento {intento}] Timeout esperando generación de precios (Tasación lenta). Reintentando...")
                    if intento < MAX_INTENTOS:
                        continue # Reiniciamos el ciclo
                    else:
                        logger.error("   ❌ Timeout final: No cargaron los precios tras espera extendida.")
                        return data

                # Helpers de limpieza locales
                def clean_clp(txt):
                    if not txt: return 0
                    clean = txt.replace('$', '').replace('.', '').strip()
                    return int(clean) if clean.isdigit() else 0

                def clean_uf(txt):
                    if not txt: return "0"
                    clean = txt.replace('UF', '').replace('.', '').strip()
                    return clean

                try:
                    # 1. VENTA
                    raw_vta_clp = driver.find_element(By.XPATH, "//h3[contains(., 'Precio estimado de venta')]/following::span[contains(@class, 'text-4xl')][1]").text
                    logger.info(f"Valor vta CLP extraido: {raw_vta_clp}")
                    raw_vta_uf = driver.find_element(By.XPATH, "//h3[contains(., 'Precio estimado de venta')]/following::span[contains(text(), 'UF')][1]").text
                    logger.info(f"Valor vta UF extraido: {raw_vta_uf}")
                    
                    # 2. ARRIENDO
                    raw_arr_clp = driver.find_element(By.XPATH, "//h3[contains(., 'Precio estimado de arriendo')]/following::span[contains(@class, 'text-4xl')][1]").text
                    logger.info(f"Valor arr CLP extraido: {raw_arr_clp}")
                    raw_arr_uf = driver.find_element(By.XPATH, "//h3[contains(., 'Precio estimado de arriendo')]/following::span[contains(text(), 'UF')][1]").text
                    logger.info(f"Valor arr UF extraido: {raw_arr_uf}")
                    
                    # Asignación
                    data["tasa_vta_clp"] = clean_clp(raw_vta_clp)
                    data["tasa_vta_uf"] = clean_uf(raw_vta_uf)
                    data["tasa_arr_clp"] = clean_clp(raw_arr_clp)
                    data["tasa_arr_uf"] = clean_uf(raw_arr_uf)
                    
                    logger.success(f"     💵 Venta: ${data['tasa_vta_clp']} ({data['tasa_vta_uf']} UF) | Arriendo: ${data['tasa_arr_clp']}")
                    
                    # Si llegamos aquí sin errores, salimos del loop y retornamos la data válida
                    return data 

                except Exception as ex_extract:
                    logger.warning(f"     ⚠️ Error extrayendo textos del DOM: {ex_extract}")
                    return data

            else:
                texto_error = elemento_resultado.text
                logger.warning(f"   ⚠️ Tasador: No encontrada ({texto_error})")
                return data # Si no existe, no tiene sentido reintentar

        except UnexpectedAlertPresentException as e:
            # CAPTURA ESPECÍFICA DE LA ALERTA "Error cargando la información"
            try:
                alert = driver.switch_to.alert
                texto_alerta = alert.text
                alert.accept()
                logger.warning(f"   🚨 [Intento {intento}/{MAX_INTENTOS}] Alerta del sitio detectada: '{texto_alerta}'. Reintentando flujo...")
            except NoAlertPresentException:
                logger.warning(f"   🚨 [Intento {intento}/{MAX_INTENTOS}] Excepción de alerta disparada, pero la alerta ya no está activa.")
            
            time.sleep(2) 
            continue 

        except Exception as e:
            logger.error(f"   ⚠️ [Intento {intento}/{MAX_INTENTOS}] Excepción general en tasador: {e}")
            if intento < MAX_INTENTOS:
                time.sleep(2)
                continue
            else:
                logger.error("   ❌ Fallaron todos los intentos de tasación.")
                return data

    return data

# ==============================================================================
#  WORKER = Basicamente el numero de navegadores que se abren al mismo tiempo
# ==============================================================================
def procesar_lote_worker(id_worker, sublista_propiedades, cancel_event):
    carpeta_worker = os.path.join(OUTPUT_FOLDER, f"temp_{id_worker}")
    if not os.path.exists(carpeta_worker):
        os.makedirs(carpeta_worker)

    driver = _configurar_driver(carpeta_worker)
    wait = WebDriverWait(driver, 15)
    
    exitos = 0
    fallidos = [] 

    try:
        if not _iniciar_sesion_hp(driver, wait):
            # Si falla la sesión, marcamos todos como error de login
            for i in sublista_propiedades:
                i['motivo_error'] = "Fallo Login"
                fallidos.append(i)
            return 0, fallidos
        
        for item in sublista_propiedades:
            if cancel_event.is_set(): break
            
            exito_item = False
            resultado = _descargar_pdf_individual(driver, wait, item, carpeta_worker)
            
            if resultado == "ROL_NOT_FOUND":
                logger.error(f"      🚫 [Worker-{id_worker}] Saltando {item['rol']}: No existe en el servidor.")
                item['motivo_error'] = "Rol no encontrado"  # <--- ETIQUETAMOS EL ERROR
                fallidos.append(item)
                continue 

            if resultado is True:
                exitos += 1
                exito_item = True
            else:
                for intento in range(1, 3):
                    if cancel_event.is_set(): break
                    logger.warning(f"      🔄 [Worker-{id_worker}] Reintento {intento+1}/3 para {item['rol']}...")
                    if _descargar_pdf_individual(driver, wait, item, carpeta_worker) is True:
                        exitos += 1
                        exito_item = True
                        break
            
            if not exito_item:
                if 'motivo_error' not in item:
                    item['motivo_error'] = "Error Descarga/Timeout" # Etiqueta genérica
                fallidos.append(item)
                
    finally:
        driver.quit()
        try: shutil.rmtree(carpeta_worker)
        except: pass
    
    return exitos, fallidos

# ==============================================================================
# ORQUESTADOR (ADAPTADO A THREADPOOLEXECUTOR)
# ==============================================================================
def orquestador_descargas(lista_propiedades, cancel_event, callback_progreso=None):
    if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)
    
    total = len(lista_propiedades)
    logger.info(f"🚀 Iniciando ciclo de descargas PARALELO para {total} propiedades. WORKERS={WORKERS}")

    chunk_size = math.ceil(total / WORKERS)
    chunks = [lista_propiedades[i:i + chunk_size] for i in range(0, total, chunk_size)]
    
    total_exitos = 0
    total_fallidos = []
    procesados_global = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            futures.append(executor.submit(procesar_lote_worker, i+1, chunk, cancel_event))
        
        for future in futures:
            try:
                e_count, f_list = future.result()
                total_exitos += e_count
                total_fallidos.extend(f_list)
                
                procesados_global += (e_count + len(f_list))
                if callback_progreso:
                    callback_progreso(procesados_global, total)

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
    
    return total_exitos, total_fallidos

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
        return 0, []
        
    clean = estandarizar_data(raw)
    if not clean: 
        logger.error("❌ No hay datos válidos después de la limpieza. Abortando.")
        return 0, []
        
    # Pasamos el callback al orquestador
    return orquestador_descargas(clean, cancel_event, callback_progreso=callback_progreso)



# if __name__ == "__main__":
#     import threading

#     import sys
#     import os
#     sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
#     from logger import get_logger


#     # Configuración
#     logger = get_logger("paso0_hp", log_dir="logs", log_file="paso0.log")
    
#     # 1. Definir archivo de entrada para la prueba
#     # Asegúrate de que este archivo exista en la raíz o ajusta la ruta
#     ARCHIVO_INPUT_TEST = "propiedades.csv" 
    
#     # 2. Crear el evento de cancelación (necesario por la firma de la función)
#     cancel_token = threading.Event()

#     # 3. Callback simple para visualizar progreso en consola
#     def reporte_progreso(procesados, total):
#         porcentaje = int((procesados / total) * 100) if total > 0 else 0
#         print(f"   📊 [Callback] Progreso: {procesados}/{total} ({porcentaje}%)")

#     print(f"🧪 MODO PRUEBA: Ejecutando solo Paso 0 con '{ARCHIVO_INPUT_TEST}'")
    
#     if os.path.exists(ARCHIVO_INPUT_TEST):
#         inicio = time.time()
        
#         # Ejecutamos
#         exitos, fallidos = ejecutar(ARCHIVO_INPUT_TEST, cancel_token, callback_progreso=reporte_progreso)
        
#         fin = time.time()
#         print(f"\n⏱️ Tiempo total: {round(fin - inicio, 2)} segundos")
#         print(f"✅ Éxitos: {exitos}")
#         print(f"❌ Fallidos: {len(fallidos)}")
#     else:
#         logger.error(f"❌ No se encontró el archivo '{ARCHIVO_INPUT_TEST}' para la prueba.")
#         print(f"⚠️ Crea un archivo llamado '{ARCHIVO_INPUT_TEST}' en la carpeta del script o edita la variable en el bloque if __name__.")