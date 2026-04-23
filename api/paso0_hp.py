import os
import time
import json
import re
import math
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

# --- DEPENDENCIAS PARA EL ABREPUERTAS ---
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from pyvirtualdisplay import Display
# -----------------------------------------------

from dotenv import load_dotenv
from logger import get_logger
from pasotasacion import obtener_tasacion

# Configuración
logger = get_logger("paso0_hp", log_dir="logs", log_file="paso0.log")
load_dotenv()

OUTPUT_FOLDER = os.path.abspath("./input_pdfs")

# Variables de entorno originales y necesarias para Requests
URL_BASE = "https://www.housepricing.cl"
URL_LOGIN = os.getenv("LOGIN_URL") or f"{URL_BASE}/login/"
URL_ANTECEDENTES = os.getenv("URL_ANTECEDENTES")
URL_TASACIONES = os.getenv("URL_TASACIONES")
EMAIL_HP = os.getenv("USUARIO_HP")
PASS_HP = os.getenv("PASSWORD_HP")

# Endpoints directos para requests
URL_BUSQUEDA_ROL = f"{URL_BASE}/search-rol/"
URL_GENERAR_INFORME = f"{URL_BASE}/informe-antecedentes/generar/" # <--- RUTA ACTUALIZADA
URL_CHECK_INFORME = f"{URL_BASE}/dashboard/informe-antecedentes-check/"

WORKERS = 3
if not URL_ANTECEDENTES or not URL_LOGIN:
    logger.error("❌ ERROR CRÍTICO: No se cargaron las URL del archivo .env")
    raise ValueError("Faltan variables de entorno en el archivo .env")

# ==============================================================================
# 1. CARGA DE DATOS (INTACTO)
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
    roles_vistos = set()

    for item in lista_cruda:
        r = str(item.get("rol", "")).strip()
        c = str(item.get("comuna", "")).strip()
        if len(r) > 3 and len(c) > 3:
            if r not in roles_vistos:
                lista_limpia.append({"rol": r, "comuna": c})
                roles_vistos.add(r)
            else:
                logger.warning(f"   ⚠️ Rol duplicado omitido para optimizar recursos: {r}")
        else:
            logger.warning(f"   ⚠️ Fila descartada por datos incompletos: Rol='{r}', Comuna='{c}'")
            
    logger.info(f"✅ Datos listos para procesar: {len(lista_limpia)} propiedades.")
    return lista_limpia


# ==============================================================================
# 2. EL ABREPUERTAS (SELENIUM PARA EVADIR TURNSTILE)
# ==============================================================================
def obtener_cookies_selenium(email, password):
    logger.info("🤖 Iniciando Selenium (Abrepuertas) en Monitor Virtual...")
    
    # INICIAMOS EL MONITOR VIRTUAL FANTASMA (Invisible pero real para Chrome)
    # En Windows esto podría dar error, está pensado para el servidor Linux
    display = None
    if os.name != 'nt': # Solo levanta Xvfb si no estamos en Windows
        display = Display(visible=0, size=(1920, 1080), color_depth=24)
        display.start()
    
    os.system("pkill -9 -f 'chrome|chromedriver' > /dev/null 2>&1")
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    
    # ❌ ELIMINAMOS --headless=new PARA SIEMPRE
    
    # Banderas necesarias para servidores Linux (estabilidad)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")

    options.page_load_strategy = 'eager'

    cookies_dict = {}
    driver = None
    
    # Helper local para simular tipeo humano
    def human_typing(element, text):
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))
            
    try:
        ruta_driver_exacto = ChromeDriverManager().install()
        
        logger.debug(f"   ⚙️ Usando ChromeDriver en: {ruta_driver_exacto}")

        # 2. Le decimos a undetected_chromedriver que use ESE driver específicamente
        driver = uc.Chrome(
            options=options,
            driver_executable_path=ruta_driver_exacto
        )
        
        driver.get(URL_LOGIN)
        
        logger.debug("   ➡️ Esperando formulario de login...")
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "email")))
        
        time.sleep(random.uniform(1.5, 3.0))
        
        # Email
        email_input = driver.find_element(By.NAME, "email")
        ActionChains(driver).move_to_element(email_input).click().perform()
        time.sleep(random.uniform(0.2, 0.6))
        human_typing(email_input, email)
        
        time.sleep(random.uniform(0.5, 1.2))
        
        # Password
        pass_input = driver.find_element(By.NAME, "password")
        ActionChains(driver).move_to_element(pass_input).click().perform()
        time.sleep(random.uniform(0.2, 0.6))
        human_typing(pass_input, password)
        
        logger.debug("   ➡️ Credenciales escritas. Esperando a que Turnstile analice la entropía...")
        time.sleep(random.uniform(3.5, 5.0)) 
        
        # Click
        submit_btn = driver.find_element(By.XPATH, "/html/body/section/div/div/section/div/div/div/div[2]/form/div[4]/button")
        ActionChains(driver).move_to_element(submit_btn).pause(random.uniform(0.5, 1.0)).click().perform()
        
        WebDriverWait(driver, 25).until(
            lambda d: "login" not in d.current_url.lower()
        )
        
        logger.success("   ✅ Login exitoso con Selenium. Extrayendo cookies...")
        
        for cookie in driver.get_cookies():
            cookies_dict[cookie['name']] = cookie['value']
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Error en el Abrepuertas Selenium: {error_msg}")
        return {}, error_msg
    finally:
        if driver:
            driver.quit() 
            logger.debug("   🧹 Instancia de Chrome cerrada.")
        if display:
            display.stop() # Importante apagar el monitor virtual
            logger.debug("   🧹 Monitor virtual apagado.")
            
    return cookies_dict, None

# ==============================================================================
# 3. LÓGICA DE DESCARGA VIA REQUESTS 
# ==============================================================================
class HousePricingClient:
    def __init__(self, worker_id="N/A", partial_error_callback=None):
        self.session = requests.Session()
        self.partial_error_callback = partial_error_callback
        self.worker_id = worker_id
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        self.csrf_token = None

    def _update_csrf_from_cookies(self):
        # Buscamos la cookie iterando para evitar el crash de múltiples cookies (CookieConflictError)
        token_encontrado = None
        for cookie in self.session.cookies:
            if cookie.name == "csrftoken":
                token_encontrado = cookie.value
                break
                
        if token_encontrado:
            self.csrf_token = token_encontrado
            self.session.headers.update({"X-CSRFToken": self.csrf_token})

    def _random_delay(self, min_s=1.5, max_s=3.5):
        time.sleep(random.uniform(min_s, max_s))

    def inyectar_cookies(self, cookies_dict):
        for name, value in cookies_dict.items():
            self.session.cookies.set(name, value)
        self._update_csrf_from_cookies()
        logger.debug(f"   💉 [Worker-{self.worker_id}] Cookies inyectadas exitosamente.")

    def buscar_y_descargar(self, rol, comuna, cancel_event):
        logger.info(f"🔎 [Worker-{self.worker_id}] Procesando: {comuna} - Rol {rol}")
        try:
            self._update_csrf_from_cookies()
            self._random_delay(1.5, 3.0)

            # 1. Buscar Rol
            rol_formateado = "-".join([p.lstrip("0") or "0" for p in str(rol).replace("−", "-").replace("–", "-").replace("—", "-").split("-")])
            
            search_data = {
                "rol": rol_formateado,
                "comuna": comuna,
                "csrfmiddlewaretoken": self.csrf_token
            }
            res_search = self.session.post(URL_BUSQUEDA_ROL, data=search_data, headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{URL_BASE}/dashboard/informe-antecedentes/"
            }, timeout=20)
            
            if res_search.status_code in [401, 403]:
                raise Exception("Acceso bloqueado por House Pricing (posible bloqueo de IP o sesión expirada).")
            elif res_search.status_code >= 500:
                raise Exception(f"El servidor de House Pricing está caído o en mantenimiento (Error HTTP {res_search.status_code}).")

            search_json = res_search.json()
            if not search_json.get("success") or not search_json.get("match"):
                logger.error(f"   ❌ [Worker-{self.worker_id}] Rol {rol_formateado} no encontrado en {comuna}")
                return "ROL_NOT_FOUND" 
            
            match = search_json["match"][0]
            self._random_delay(1.0, 2.5) 
            
            # 2. Generar Informe
            report_payload = {
                "csrfmiddlewaretoken": self.csrf_token,
                "rol": match["rol"],
                "codigo_sii_comuna": match["codigo_sii_comuna"],
                "latitude": match["latitude"],
                "longitude": match["longitude"],
                "address": match["address"],
                "address_comuna": match["comuna"]
            }
            
            res_trigger = self.session.post(URL_GENERAR_INFORME, data=report_payload, headers={
                "HX-Request": "true",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{URL_BASE}/"
            }, timeout=20)
            
            poll_match = re.search(r'hx-get="(/informe-antecedentes/[\w/]+/check/)"', res_trigger.text)
            if not poll_match:
                logger.error(f"   ❌ [Worker-{self.worker_id}] No se encontró el link de seguimiento (Polling).")
                raise Exception("Error técnico: No se encontró el enlace de generación del documento. Posible cambio en la web de House Pricing.")
            
            poll_url = f"{URL_BASE}{poll_match.group(1)}"
            logger.info(f"   ⏳ [Worker-{self.worker_id}] Informe en proceso. Iniciando polling...")

            # 3. Polling hasta encontrar el enlace del Informe Final
            ruta_informe_web = None
            for _ in range(35): # Aprox 2.5 minutos de tolerancia
                if cancel_event.is_set(): return False
                
                time.sleep(4)
                res_check = self.session.get(poll_url, headers={
                    "HX-Request": "true",
                    "X-Requested-With": "XMLHttpRequest"
                }, timeout=15)
                
                soup_check = BeautifulSoup(res_check.text, "html.parser")
                # Buscamos un tag <a> cuyo href comience con /informe-antecedentes/ y termine en /
                link_informe = soup_check.find("a", href=re.compile(r'^/informe-antecedentes/\d+/$'))
                
                if link_informe:
                    ruta_informe_web = link_informe["href"]
                    break
                    
                logger.debug(f"      ...[Worker-{self.worker_id}] aún procesando...")

            if not ruta_informe_web:
                logger.error(f"   ❌ [Worker-{self.worker_id}] Timeout: El servidor no entregó el link hacia el informe final.")
                raise Exception("Tiempo de espera agotado (Timeout). House Pricing no generó el link del informe a tiempo.")

            self._random_delay(0.5, 1.5) 

            # 4. Obtener Informe Web (HTML)
            url_informe_final = f"{URL_BASE}{ruta_informe_web}"
            logger.debug(f"   📥 [Worker-{self.worker_id}] Descargando Informe Web (HTML) desde {url_informe_final}...")
            res_html = self.session.get(url_informe_final, timeout=20)
            res_html.raise_for_status()

            nombre_base = f"{comuna}_{rol_formateado}".replace(" ", "_").replace("/", "-")
            ruta_html = os.path.join(OUTPUT_FOLDER, f"{nombre_base}.html")
            
            with open(ruta_html, "w", encoding="utf-8") as f:
                f.write(res_html.text)
            
            logger.success(f"   ✅ [Worker-{self.worker_id}] HTML del informe guardado: {nombre_base}.html")

            # 5. Fallback: Buscar y Descargar PDF desde el HTML final
            soup_final = BeautifulSoup(res_html.text, "html.parser")
            link_pdf = soup_final.find("a", href=re.compile(r'\.pdf', re.IGNORECASE))
            
            if link_pdf and link_pdf.get("href"):
                pdf_url = link_pdf["href"]
                logger.debug(f"   📥 [Worker-{self.worker_id}] Link PDF detectado. Descargando archivo de respaldo...")
                
                res_pdf = self.session.get(pdf_url, stream=True, timeout=20)
                res_pdf.raise_for_status()
                
                ruta_pdf = os.path.join(OUTPUT_FOLDER, f"{nombre_base}.pdf")
                tamanio_descargado = 0
                with open(ruta_pdf, "wb") as f:
                    for chunk in res_pdf.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            tamanio_descargado += len(chunk)
                            
                if tamanio_descargado > 0:
                    logger.success(f"   ✅ [Worker-{self.worker_id}] PDF de respaldo guardado: {nombre_base}.pdf")
                else:
                    logger.warning(f"   ⚠️ [Worker-{self.worker_id}] El PDF se descargó vacío (0 KB).")
            else:
                logger.warning(f"   ⚠️ [Worker-{self.worker_id}] No se encontró botón de descarga PDF en el HTML.")

            # 6. Metadata Mínima (Sin tasación, Paso 1 se encargará de extraerla del HTML)
            ruta_json = os.path.join(OUTPUT_FOLDER, f"{nombre_base}.pdf.json") # Mantenemos nombre para compatibilidad del script
            
            datos_tasacion = obtener_tasacion(
                session=self.session,
                match_data=match,
                csrf_token=self.csrf_token,
                url_base=URL_BASE,
                worker_id=self.worker_id
            )

            if datos_tasacion.get("motivo_error") and self.partial_error_callback:
                self.partial_error_callback(
                    rol=rol_formateado, 
                    paso="Tasación (Paso 0)", 
                    motivo=datos_tasacion.get("motivo_error")
                )
            
            meta_data = {
                "link_informe": url_informe_final,
                "rol_origen": rol_formateado,
                "comuna_origen": comuna,
                "tasa_vta_clp": datos_tasacion.get("tasa_vta_clp", 0),
                "tasa_vta_uf": datos_tasacion.get("tasa_vta_uf", "0"),
                "tasa_arr_clp": datos_tasacion.get("tasa_arr_clp", 0),
                "tasa_arr_uf": datos_tasacion.get("tasa_arr_uf", "0")
            }
            with open(ruta_json, "w", encoding="utf-8") as f:
                json.dump(meta_data, f)

            return True

        except requests.exceptions.Timeout:
            logger.warning(f"   ⏳ [Worker-{self.worker_id}] Timeout en la red.")
            raise Exception("Tiempo de espera de red agotado. Los servidores de House Pricing están lentos o no responden.")
        except requests.exceptions.RequestException as e:
            logger.warning(f"   🔌 [Worker-{self.worker_id}] Falla de conexión HTTP: {e}")
            raise Exception(f"Error de conexión HTTP: {str(e)}")
        except Exception as e:
            logger.error(f"   ❌ [Worker-{self.worker_id}] Error inesperado en requests: {e}")
            raise

# ==============================================================================
# WORKER ADAPTADO A REQUESTS + COOKIES INYECTADAS
# ==============================================================================
def procesar_lote_worker(id_worker, sublista_propiedades, cancel_event, cookies_auth):
    client = HousePricingClient(worker_id=id_worker)
    client.inyectar_cookies(cookies_auth)
    exitos = 0
    fallidos = [] 

    try:
        for item in sublista_propiedades:
            if cancel_event.is_set(): break
            
            exito_item = False
            motivo_fallo = "Error desconocido de red." 
            
            try:
                resultado = client.buscar_y_descargar(item['rol'], item['comuna'], cancel_event)
                
                if resultado == "ROL_NOT_FOUND":
                    logger.error(f"      🚫 [Worker-{id_worker}] Saltando {item['rol']}: No existe en el servidor.")
                    item['motivo_error'] = "El rol ingresado no existe en los registros de House Pricing para esta comuna."
                    fallidos.append(item)
                    continue 

                if resultado is True:
                    exitos += 1
                    exito_item = True
                elif resultado is False:
                    raise Exception("Descarga interrumpida: Cancelación solicitada por el usuario durante el polling.")
                else:
                    raise Exception(f"Respuesta inesperada del método de descarga: {resultado}")
            
            except Exception as error_inicial:
                motivo_fallo = str(error_inicial)
                
                # Reintentos
                for intento in range(1, 3):
                    if cancel_event.is_set(): break
                    logger.warning(f"      🔄 [Worker-{id_worker}] Reintento {intento+1}/3 para {item['rol']}... (Previo: {motivo_fallo})")
                    client._random_delay(5.0, 10.0) 
                    
                    try:
                        resultado = client.buscar_y_descargar(item['rol'], item['comuna'], cancel_event)
                        
                        if resultado is True:
                            exitos += 1
                            exito_item = True
                            break
                        elif resultado == "ROL_NOT_FOUND":
                            motivo_fallo = "El rol ingresado no existe en los registros de House Pricing para esta comuna."
                            break
                        elif resultado is False:
                            raise Exception("Reintento interrumpido: Cancelación solicitada por el usuario.")
                        else:
                            raise Exception(f"Fallo en reintento. Respuesta inesperada: {resultado}")
                    except Exception as error_reintento:
                        motivo_fallo = str(error_reintento)
            
            if not exito_item:
                if 'motivo_error' not in item:
                    item['motivo_error'] = motivo_fallo
                fallidos.append(item)
                
    except Exception as e:
        logger.error(f"Error crítico en Worker-{id_worker}: {e}")
    
    return exitos, fallidos

# ==============================================================================
# ORQUESTADOR (INYECCIÓN DE ABREPUERTAS)
# ==============================================================================
def orquestador_descargas(lista_propiedades, cancel_event, callback_progreso=None):
    if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)
    
    total = len(lista_propiedades)
    logger.info(f"🚀 Iniciando ciclo de descargas PARALELO para {total} propiedades. WORKERS={WORKERS}")

    cookies_auth, error_selenium = obtener_cookies_selenium(EMAIL_HP, PASS_HP)
    
    if not cookies_auth or "sessionid" not in cookies_auth:
        detalle_error = error_selenium if error_selenium else "No se detectó cookie sessionid. Posible bloqueo o credenciales inválidas."
        logger.error(f"❌ Fallo crítico de inicialización. Detalle técnico: {detalle_error}")
        
        fallidos = []
        for p in lista_propiedades:
            p['motivo_error'] = f"Fallo en Selenium/Abrepuertas: {detalle_error}"
            fallidos.append(p)
            
        # --- MODIFICADO: Agregamos None al final ---
        return 0, fallidos, None

    chunk_size = math.ceil(total / WORKERS)
    chunks = [lista_propiedades[i:i + chunk_size] for i in range(0, total, chunk_size)]
    
    total_exitos = 0
    total_fallidos = []
    procesados_global = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            futures.append(executor.submit(procesar_lote_worker, i+1, chunk, cancel_event, cookies_auth))
        
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
    
    # --- MODIFICADO: Retornamos las cookies_auth ---
    return total_exitos, total_fallidos, cookies_auth

# ==============================================================================
# ENTRY POINT
# ==============================================================================
def ejecutar(ruta_archivo: str, cancel_event, callback_progreso=None, partial_error_callback=None):
    logger.info(f"=== PASO 0: INICIO DEL FLUJO DE DESCARGAS ===")
    logger.info(f"📂 Archivo de entrada: {ruta_archivo}")
    
    raw = detectar_y_cargar(ruta_archivo)
    if not raw: 
        logger.error("❌ No se pudieron cargar los datos iniciales. Abortando.")
        # --- MODIFICADO ---
        return 0, [], None 
        
    clean = estandarizar_data(raw)
    if not clean: 
        logger.error("❌ No hay datos válidos después de la limpieza. Abortando.")
        # --- MODIFICADO ---
        return 0, [], None 
        
    return orquestador_descargas(clean, cancel_event, callback_progreso=callback_progreso)