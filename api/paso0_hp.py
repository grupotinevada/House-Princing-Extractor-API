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
from pasotasacion import obtener_tasacion

from dotenv import load_dotenv
from logger import get_logger

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
URL_GENERAR_INFORME = f"{URL_BASE}/dashboard/informe-antecedentes-resultado/"
URL_CHECK_INFORME = f"{URL_BASE}/dashboard/informe-antecedentes-check/"

WORKERS = 5 

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
# 2 y 3. LÓGICA DE DESCARGA VIA REQUESTS (REEMPLAZO SELENIUM)
# ==============================================================================
class HousePricingClient:
    def __init__(self, worker_id="N/A"):
        self.session = requests.Session()
        self.worker_id = worker_id
        # Cabeceras robustas para simular navegador real
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        self.csrf_token = None

    def _update_csrf_from_cookies(self):
        self.csrf_token = self.session.cookies.get("csrftoken")
        if self.csrf_token:
            self.session.headers.update({"X-CSRFToken": self.csrf_token})

    def _random_delay(self, min_s=1.5, max_s=3.5):
        """Previene rate-limiting simulando tiempos de interacción humana."""
        time.sleep(random.uniform(min_s, max_s))

    def login(self, email, password):
        logger.info(f"🔐 [Worker-{self.worker_id}] Intentando login para: {email}")
        
        login_form_url = f"{URL_BASE}/login-service/?next=/dashboard/"
        login_service_url = f"{URL_BASE}/login-service/"

        # Simular llegada a la web principal
        self.session.get(URL_BASE)
        self._random_delay(1.0, 2.0)

        logger.debug(f"   ➡️ [Worker-{self.worker_id}] Pidiendo el formulario de login dinámico...")
        res_form = self.session.get(
            login_form_url,
            headers={
                "Referer": f"{URL_BASE}/login/",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "*/*"
            }
        )

        self.csrf_token = self.session.cookies.get("csrftoken")
        soup = BeautifulSoup(res_form.text, "html.parser")
        token_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
        
        if token_input:
            self.csrf_token = token_input["value"]
        elif not self.csrf_token:
            logger.error(f"❌ [Worker-{self.worker_id}] Fracasó la obtención del CSRF.")
            return False

        self._random_delay(2.0, 4.0) # Tiempo humano llenando credenciales

        data = {
            "csrfmiddlewaretoken": self.csrf_token,
            "next": "/dashboard/",
            "email": email,
            "password": password
        }

        logger.debug(f"   ➡️ [Worker-{self.worker_id}] Enviando credenciales...")
        res_post = self.session.post(
            login_service_url, 
            data=data, 
            headers={
                "Referer": f"{URL_BASE}/login/",
                "Origin": URL_BASE,
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
            }
        )
        
        if "sessionid" in self.session.cookies:
            logger.success(f"✅ [Worker-{self.worker_id}] Login exitoso via Requests.")
            self._update_csrf_from_cookies()
            return True
            
        try:
            respuesta_json = res_post.json()
            if respuesta_json.get("success", False) or "redirect" in res_post.text:
                logger.success(f"✅ [Worker-{self.worker_id}] Login exitoso via Requests (JSON OK).")
                self._update_csrf_from_cookies()
                return True
        except:
            pass
            
        logger.error(f"❌ [Worker-{self.worker_id}] Falló el login final. HTTP Code: {res_post.status_code}")
        return False

    def buscar_y_descargar(self, rol, comuna, cancel_event):
        logger.info(f"🔎 [Worker-{self.worker_id}] Procesando: {comuna} - Rol {rol}")
        try:
            self._update_csrf_from_cookies()
            self._random_delay(1.5, 3.0) # Pausa entre búsquedas

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
            })
            
            search_json = res_search.json()
            if not search_json.get("success") or not search_json.get("match"):
                logger.error(f"   ❌ [Worker-{self.worker_id}] Rol {rol_formateado} no encontrado en {comuna}")
                return "ROL_NOT_FOUND" # Respetando validación original
            
            match = search_json["match"][0]
            self._random_delay(1.0, 2.5) # Simula tiempo de ver resultados
            
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
                "Referer": f"{URL_BASE}/dashboard/informe-antecedentes/"
            })
            
            poll_match = re.search(r'hx-get="(/dashboard/informe-antecedentes-check/[\w/]+)"', res_trigger.text)
            if not poll_match:
                logger.error(f"   ❌ [Worker-{self.worker_id}] No se encontró el link de seguimiento (Polling).")
                return False
            
            poll_url = f"{URL_BASE}{poll_match.group(1)}"
            logger.info(f"   ⏳ [Worker-{self.worker_id}] Informe en proceso. Iniciando polling...")

            # 3. Polling
            pdf_url = None
            for _ in range(35): # Aprox 2.5 minutos de tolerancia
                if cancel_event.is_set(): return False
                
                time.sleep(4)
                res_check = self.session.get(poll_url, headers={
                    "HX-Request": "true",
                    "X-Requested-With": "XMLHttpRequest"
                })
                
                if ".pdf" in res_check.text:
                    soup_check = BeautifulSoup(res_check.text, "html.parser")
                    pdf_url = soup_check.find("a", href=True)["href"]
                    break
                logger.debug(f"      ...[Worker-{self.worker_id}] aún procesando...")

            if not pdf_url:
                logger.error(f"   ❌ [Worker-{self.worker_id}] Timeout: El servidor no entregó el PDF a tiempo.")
                return False

            self._random_delay(0.5, 1.5) # Pausa antes de la descarga

            # 4. Descarga de PDF Directa a Memoria -> Disco
            logger.debug(f"   📥 [Worker-{self.worker_id}] Descargando PDF desde link final...")
            res_pdf = self.session.get(pdf_url, stream=True)
            res_pdf.raise_for_status()

            nombre_archivo = f"{comuna}_{rol_formateado}.pdf".replace(" ", "_").replace("/", "-")
            ruta_pdf = os.path.join(OUTPUT_FOLDER, nombre_archivo)
            
            with open(ruta_pdf, "wb") as f:
                for chunk in res_pdf.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # 5. Metadata (Tasaciones a 0 como se especificó en la lógica request)
            datos_tasacion = obtener_tasacion(
                session=self.session, 
                match_data=match, 
                csrf_token=self.csrf_token, 
                url_base=URL_BASE, 
                worker_id=self.worker_id
            )

            ruta_json = ruta_pdf + ".json"
            meta_data = {
                "link_informe": pdf_url,
                "rol_origen": rol_formateado,
                "comuna_origen": comuna,
                "tasa_vta_clp": datos_tasacion["tasa_vta_clp"],
                "tasa_vta_uf": datos_tasacion["tasa_vta_uf"],
                "tasa_arr_clp": datos_tasacion["tasa_arr_clp"],
                "tasa_arr_uf": datos_tasacion["tasa_arr_uf"]
            }
            with open(ruta_json, "w", encoding="utf-8") as f:
                json.dump(meta_data, f)

            logger.success(f"   ✅ [Worker-{self.worker_id}] PDF y metadata guardados: {nombre_archivo}")
            return True

        except Exception as e:
            logger.error(f"   ❌ [Worker-{self.worker_id}] Error inesperado en requests: {e}")
            return False

# ==============================================================================
# WORKER ADAPTADO A REQUESTS
# ==============================================================================
def procesar_lote_worker(id_worker, sublista_propiedades, cancel_event):
    client = HousePricingClient(worker_id=id_worker)
    exitos = 0
    fallidos = [] 

    try:
        if not client.login(EMAIL_HP, PASS_HP):
            for i in sublista_propiedades:
                i['motivo_error'] = "Fallo Login"
                fallidos.append(i)
            return 0, fallidos
        
        for item in sublista_propiedades:
            if cancel_event.is_set(): break
            
            exito_item = False
            resultado = client.buscar_y_descargar(item['rol'], item['comuna'], cancel_event)
            
            if resultado == "ROL_NOT_FOUND":
                logger.error(f"      🚫 [Worker-{id_worker}] Saltando {item['rol']}: No existe en el servidor.")
                item['motivo_error'] = "Rol no encontrado"
                fallidos.append(item)
                continue 

            if resultado is True:
                exitos += 1
                exito_item = True
            else:
                for intento in range(1, 3):
                    if cancel_event.is_set(): break
                    logger.warning(f"      🔄 [Worker-{id_worker}] Reintento {intento+1}/3 para {item['rol']}...")
                    client._random_delay(5.0, 10.0) # Castigo de tiempo si falló (Anti-block)
                    
                    if client.buscar_y_descargar(item['rol'], item['comuna'], cancel_event) is True:
                        exitos += 1
                        exito_item = True
                        break
            
            if not exito_item:
                if 'motivo_error' not in item:
                    item['motivo_error'] = "Error Descarga/Timeout"
                fallidos.append(item)
                
    except Exception as e:
        logger.error(f"Error crítico en Worker-{id_worker}: {e}")
    
    return exitos, fallidos

# ==============================================================================
# ORQUESTADOR (INTACTO)
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
# ENTRY POINT (INTACTO)
# ==============================================================================
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
        
    return orquestador_descargas(clean, cancel_event, callback_progreso=callback_progreso)