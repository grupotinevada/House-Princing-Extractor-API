############################################################################################################################
#  El paso 2 por medio de SELENIUM abrimos la pagina de House Princing para poder extraer las propiedades comparables del rol y comuna ingresado
#  Viaja por la pagina , ingresa los datos y extrae la data de las propiedades comparables
#  Guarda la data en el JSON proveniente del paso 1
############################################################################################################################
############################################################################################################################
#   ACUERDATE DE EJECUTAR EL PROCESO Y MANDAR EL LOG AL CHAT DE GEMINIS PARA QUE TE EVALUE LOS ERRORES Y SI ESTA FUNCIONANDO LA LOGICA QUE CREAMOS
############################################################################################################################
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed # Para paralelismo

from logger import get_logger, log_section, dbg
logger = get_logger("paso2_hp", log_dir="logs", log_file="paso2_hp.log")

from dotenv import load_dotenv
import os 


load_dotenv()

# --- CONFIGURACIÓN ---
EMAIL = os.getenv("USUARIO_HP")
PASSWORD = os.getenv("PASSWORD_HP")
LOGIN_URL = os.getenv("LOGIN_URL")
BUSQUEDA_URL = os.getenv("BUSQUEDA_URL")
WORKERS = 2  # Estandarizado con Paso 0

logger.info(f"⚙️ Configuración cargada. Usuario: {EMAIL} | Workers: {WORKERS}")

def generar_link_maps(lat, lng):
    """Genera link directo a Google Maps con pin en la coordenada"""
    if not lat or not lng:
        return None
    # Formato estándar: https://www.google.com/maps?q=LAT,LNG
    return f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lng}"


def calcular_distancia(lat1, lon1, lat2, lon2):
    """Calcula metros entre dos puntos (Fórmula de Haversine)"""
    if lat1 is None or lat2 is None: return 99999999
    
    R = 6371000 
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    dist = int(R*c)
    # logger.debug(f"📏 Distancia calculada: {dist} mts") 
    return dist

def parse_propiedades(html, cancel_event,fuente_actual):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".hpid")
    
    logger.debug(f"   🧩 [BS4] Iniciando parsing HTML para '{fuente_actual}'. Cards detectadas: {len(cards)}")
    
    resultados = []
    for idx, card in enumerate(cards):
        
        if cancel_event.is_set():
            logger.warning("🛑 Parsing interrumpido por evento de cancelación.")
            return []
        try:

            raw_name = card.get("data-name")                # Puede ser Calle o Link
            raw_display = card.get("data-display-name")     # El "Plan B" para la dirección
            direccion_final, link_final = extraer_direccion_y_link(raw_name, raw_display)

            # 1. Extracción de Atributos Crudos
            lat_str = card.get("data-lat")
            lng_str = card.get("data-lng")
            price_fmt = card.get("data-price-formatted")
            uf_m2_fmt = card.get("data-ufm2-formatted")
            rol = card.get("data-rol")
            comuna = card.get("data-comuna")
            fecha_transaccion = card.get("data-date-trx")
            # 2. Limpieza de datos numéricos
            m2_util = card.get("data-m2-formatted")
            m2_total = card.get("data-m2-total-formatted")
            dormitorios = card.get("data-bed")
            banios = card.get("data-bath")
            anio = int(card.get("data-year")) if card.get("data-year") else 0
            # Conversión segura a float para cálculos
            lat_float = float(lat_str) if lat_str else None
            lng_float = float(lng_str) if lng_str else None

            # Construcción del objeto de datos con TODA LA INFO
            data = {
                "fuente": fuente_actual,  # <--- Guardamos si es Compraventa u Oferta
                "rol": rol,
                "direccion": direccion_final,
                "comuna": comuna,
                "lat": lat_float,
                "lng": lng_float,
                "link_maps": generar_link_maps(lat_str, lng_str),
                "precio_uf": price_fmt,
                "uf_m2": uf_m2_fmt,
                "fecha_transaccion": fecha_transaccion,
                "anio": anio,
                "m2_util": m2_util or "0",
                "m2_total": m2_total or "0",
                "dormitorios": dormitorios,
                "banios": banios,
                "distancia_metros": 999999, 
                "link_publicacion": link_final,
            }
            resultados.append(data)
            # logger.debug(f"     ✅ Card parseada: Rol {rol} - {price_fmt}") 
        except Exception as e:
            logger.warning(f"     ⚠️ Error parseando card #{idx}: {e}")
            continue
            
    logger.debug(f"   ✨ [BS4] Parsing finalizado. {len(resultados)} propiedades extraídas correctamente.")
    return resultados

def extraer_direccion_y_link(raw_name, raw_display_name):   
    """
    Analiza el atributo 'data-name' para determinar si es una dirección física o una URL.
    Retorna una tupla: (direccion_limpia, link_detectado)
    """
    # Validación básica por si viene None
    if not raw_name:
        return "Sin dirección", None

    raw_name_clean = raw_name.strip()
    
    if raw_name_clean.lower().startswith("http") or "www." in raw_name_clean.lower():
        link = raw_name_clean # Usamos el link limpio sin espacios
        direccion = raw_display_name
        if not direccion:
            direccion = "No hay dato, Ver publicacion"
        if not link:
            link = "No hay dato, Ver publicacion"
        logger.debug(f"       🔗 Link de la publicación detectado en data-name: {link[:30]}...")
        return direccion, link
    else:
        # Si no es link, asumimos que 'raw_name' es la dirección física
        direccion = raw_name_clean 
        link = None 
        return direccion, link

def aplicar_filtro_ofertas_publicadas(driver, wait):
    """
    Abre el panel lateral de filtros, selecciona 'Publicado' -> 'Sí' y aplica los cambios.
    Exclusivo para la sección de Ofertas.
    """
    logger.info("     ⚙️ Aplicando filtro adicional: Solo Ofertas 'Publicadas' activamente...")
    try:
        # 1. Abrir panel de filtros
        btn_filtros = wait.until(EC.element_to_be_clickable((By.ID, "filters-panel-open-button")))
        driver.execute_script("arguments[0].click();", btn_filtros)
        time.sleep(1) # Espera a que termine la animación CSS del panel lateral
        
        # 2. Seleccionar 'Publicado' = Sí (value '1')
        select_publicado_elem = wait.until(EC.presence_of_element_located((By.ID, "publicado")))
        Select(select_publicado_elem).select_by_value("1")
        
        # Identificamos la lista actual para rastrear cuándo se refresca
        try:
            lista_vieja = driver.find_element(By.ID, "property_list")
        except:
            lista_vieja = None

        # 3. Clic en Aplicar filtros (Buscado por su atributo onclick único)
        btn_aplicar = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@onclick='applyFiltersPanel()']")))
        driver.execute_script("arguments[0].click();", btn_aplicar)
        
        # 4. Sincronización: Esperar recarga HTMX
        if lista_vieja:
            wait.until(EC.staleness_of(lista_vieja))
        
        wait.until(EC.presence_of_element_located((By.ID, "property_list")))
        time.sleep(2) # Respiro para que el DOM pinte las nuevas cards
        
        logger.debug("     ✅ Filtro de 'Publicado' aplicado exitosamente.")
        
    except Exception as e:
        # Si falla (cambió el HTML o el sitio anda lento), no matamos el script, solo advertimos
        logger.warning(f"     ⚠️ No se pudo aplicar el filtro de 'Publicado'. Continuando con lista estándar. Detalle: {e}")


def _buscar_propiedad_individual(driver, wait, comuna_nombre, tipo_target, rol_target, cancel_event):
    from selenium.common.exceptions import TimeoutException
    
    logger.info(f"🔎 Buscando: {comuna_nombre} | Rol: {rol_target} | Tipo: {tipo_target}")
    
    datos_retorno = {
        "lat_centro": None, 
        "lng_centro": None, 
        "resultados": [], 
        "mensaje": "OK" 
    }
    
    try:
        # --- [BLOQUE A, B, C: BÚSQUEDA INICIAL] ---
        driver.get(BUSQUEDA_URL)
        logger.info(f"   ➡️ Navegando a URL de búsqueda...")
        
        select_tipo = wait.until(EC.element_to_be_clickable((By.ID, "search-type")))
        Select(select_tipo).select_by_value("rol")
        try:
            wait.until(EC.visibility_of_element_located((By.ID, "rol-container")))
        except TimeoutException:
            logger.warning(f"   ⚠️ Timeout esperando formulario para {rol_target}. Solicitando reintento...")
            raise
        # time.sleep(1) # Pequeña pausa eliminada, Selenium maneja el ritmo

        logger.info("   🖱️ Seleccionando comuna...")
        select_comuna = driver.find_element(By.ID, "select-comuna")
        driver.execute_script("arguments[0].style.display = 'block';", select_comuna)
        Select(select_comuna).select_by_visible_text(comuna_nombre)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", select_comuna)
        # time.sleep(1)

        logger.info(f"   ⌨️ Ingresando Rol {rol_target}...")
        input_rol = driver.find_element(By.ID, "inputRol")
        input_rol.clear()
        input_rol.send_keys(rol_target)
        # time.sleep(1)
        
        select_prop_type = wait.until(EC.element_to_be_clickable((By.ID, "tipo_propiedad")))
        Select(select_prop_type).select_by_visible_text(tipo_target)
        
        # Esperamos a que la página esté "tranquila" antes de buscar
        wait.until(lambda d: d.execute_script("return document.readyState === 'complete'"))

        
        # 1. Capturar contenedor viejo
        lista_vieja = None
        try:
            # Usamos ID 'property_list' 
            lista_vieja = driver.find_element(By.ID, "property_list")
            logger.debug(f"   👀 Contenedor viejo detectado (ID: {lista_vieja.id}).")
        except Exception:
            # Si por alguna razón no existe al inicio, esperamos que aparezca el nuevo directamente
            logger.debug("   👀 No se detectó contenedor viejo (limpio).")
            pass 

        # 2. Click en Buscar (Dispara el evento HTMX)
        logger.info("   🖱️ Click en botón Buscar...")
        driver.execute_script("arguments[0].click();", driver.find_element(By.ID, "btn-search-rol"))
        logger.info(f"   🚀 Request enviado para Rol: {rol_target}...")

        # 3. Sincronización: Esperar el "Parpadeo" del contenedor
        try:
            # A) Si existía la lista vieja, esperar a que MUERA (se desvincule del DOM)
            if lista_vieja:
                wait.until(EC.staleness_of(lista_vieja))
                logger.debug("   🔄 Contenedor antiguo destruido (DOM Refresh iniciado).")
            
            # B) Esperar a que NAZCA la nueva lista (el servidor respondió)
            # Esto ocurrirá haya 0 o 100 resultados.
            nueva_lista = wait.until(EC.presence_of_element_located((By.ID, "property_list")))
            logger.debug("   🆕 Nuevo contenedor 'property_list' cargado exitosamente.")

            # 4. VERIFICACIÓN RÁPIDA DE RESULTADOS (Evitar Timeout si es 0)
            # En tu imagen se ve el atributo data-total-count="300"
            try:
                total_count = nueva_lista.get_attribute("data-total-count")
                if total_count and int(total_count) == 0:
                    logger.warning(f"   ⚠️ La búsqueda terminó correctamente pero hay 0 resultados (Data del sitio).")
                    datos_retorno["mensaje"] = "Sin resultados (Fuente oficial)"
                    # Intentamos sacar el centroide igual por si acaso el mapa se movió
                    # pero no entramos a buscar cards
                else:
                    logger.info(f"   🔢 Resultados encontrados según atributo: {total_count}")
            except:
                pass # Si no tiene el atributo, seguimos al método clásico

        except TimeoutException:
            logger.error("   ❌ Timeout esperando que se refresque #property_list.")
            datos_retorno["mensaje"] = "Error de carga (Timeout)"
            raise   

        # --- [EXTRACCIÓN DE CENTROIDE Y DATOS] ---
        # Solo intentamos buscar .hpid si sabemos que hay algo o si falló la lectura del count
        if datos_retorno["mensaje"] == "OK":
            try:
                # Damos un respiro mínimo para que el renderizado interno termine
                # (A veces el div padre está, pero los hijos tardan milisegundos en pintar)
                time.sleep(2) 
                
                ne_lat = driver.find_element(By.NAME, "ne_lat").get_attribute("value")
                ne_lng = driver.find_element(By.NAME, "ne_lng").get_attribute("value")
                sw_lat = driver.find_element(By.NAME, "sw_lat").get_attribute("value")
                sw_lng = driver.find_element(By.NAME, "sw_lng").get_attribute("value")

                if ne_lat and sw_lat:
                    datos_retorno["lat_centro"] = (float(ne_lat) + float(sw_lat)) / 2
                    datos_retorno["lng_centro"] = (float(ne_lng) + float(sw_lng)) / 2
                    logger.debug(f"   📍 Centroide calculado: {datos_retorno['lat_centro']:.5f}, {datos_retorno['lng_centro']:.5f}")
            except Exception:
                logger.warning(f"   ⚠️ No se pudieron extraer coordenadas del mapa para {rol_target}")

            # --- [BLOQUE ITERAR FUENTES] ---
            # Solo entramos aquí si NO detectamos 0 resultados arriba
            lista_total = []
            fuentes_a_extraer = ["Compraventas", "Ofertas"] 
            
            for fuente_val in fuentes_a_extraer:
                if cancel_event.is_set(): return datos_retorno
                
                logger.info(f"   --- 🔄 Cambiando a fuente: {fuente_val} ---")
                
                try:
                    # 1. Seleccionar la fuente (Esto TAMBIÉN refresca la lista, ojo)
                    select_elem = wait.until(EC.element_to_be_clickable((By.ID, "fuente")))
                    Select(select_elem).select_by_value(fuente_val)
                    
                    # Esperamos recarga
                    logger.debug(f"     ⏳ Esperando recarga de filtro {fuente_val}...")
                    time.sleep(3) 

                    # 2. Re-aplicar orden
                    try:
                        sort_select = driver.find_element(By.ID, "sort-selector")
                        Select(sort_select).select_by_value("year_desc")
                        time.sleep(2)
                        logger.debug("     🔽 Orden aplicado: Año descendente")
                    except:
                        logger.debug("     ℹ️ No se encontró selector de orden.")
                        pass 

                    if fuente_val == "Ofertas":
                        aplicar_filtro_ofertas_publicadas(driver, wait) # Solo para Ofertas, aplicamos el filtro extra de "Publicado = Sí"

                    #3. Parsear (Extrae todas las cards cargadas en la web, ej: 50 propiedades)
                    propiedades_raw = parse_propiedades(driver.page_source, cancel_event, fuente_val)
                    
                    # --- FILTRO ESTRICTO DE LINKS ---
                    if propiedades_raw and fuente_val == "Ofertas":
                        con_link = [p for p in propiedades_raw if p.get("link_publicacion") and p["link_publicacion"] != "No hay dato, Ver publicacion"]
                        
                        # Si es Oferta, trajo cards, pero NINGUNA tiene link -> DOM Incompleto
                        if len(con_link) == 0:
                            logger.error(f"     ❌ [DOM INCOMPLETO] {len(propiedades_raw)} cards en Ofertas pero NINGUNA tiene link. Forzando recarga.")
                            raise Exception("DOM_INCOMPLETO_LINK_FALTANTE")
                        
                        descartadas = len(propiedades_raw) - len(con_link)
                        if descartadas > 0:
                            logger.warning(f"     ⚠️ Se omitieron {descartadas} propiedades sin link en Ofertas.")
                            
                        propiedades_raw = con_link
                    # --------------------------------------------------

                    # 4. Calcular distancias (Se aplica solo a las sobrevivientes válidas)
                    if datos_retorno["lat_centro"]:
                        logger.debug(f"     📐 Calculando distancias para {len(propiedades_raw)} propiedades válidas...")
                        for p in propiedades_raw:
                            p['distancia_metros'] = calcular_distancia(
                                datos_retorno["lat_centro"], datos_retorno["lng_centro"], 
                                p['lat'], p['lng']
                            )
                        # Ordenamos de la más cercana a la más lejana
                        propiedades_raw = sorted(propiedades_raw, key=lambda x: x.get('distancia_metros', 999999))
                    
                    # 5. Cortar las mejores 10 (Garantiza 10 propiedades SÍ O SÍ con link, si hay disponibles)
                    mejores_10 = propiedades_raw[:10]
                    lista_total.extend(mejores_10)
                    
                    logger.success(f"     📥 Se agregaron {len(mejores_10)} propiedades válidas (Top 10 más cercanas) de {fuente_val}")

                except Exception as e:
                    # --- NUEVO MANEJO DE ERROR ---
                    # Si es nuestro error de DOM incompleto, lo lanzamos hacia arriba para forzar el reintento del rol.
                    if "DOM_INCOMPLETO" in str(e):
                        raise
                    # Si es otro error menor, lo logueamos y seguimos con la otra fuente.
                    logger.error(f"     ❌ Error procesando fuente {fuente_val}: {e}", exc_info=True)
            
            datos_retorno["resultados"] = lista_total
            
            if not datos_retorno["resultados"] and datos_retorno["mensaje"] == "OK":
                datos_retorno["mensaje"] = "Sin resultados en ninguna fuente"
                logger.warning("   ⚠️ Finalizado sin resultados en Compraventas ni Ofertas.")
    except TimeoutException:
        raise
    except Exception as e:
        logger.error(...)
        datos_retorno["mensaje"] = f"Error técnico: {str(e)}"

    
    return datos_retorno

# ==============================================================================
# NUEVO: WORKER ESTANDARIZADO (CADA WORKER TIENE SU NAVEGADOR)
# ==============================================================================
def procesar_lote_worker(id_worker, sublista_propiedades, cancel_event, callback_progreso=None):
    """
    Función Worker que se ejecuta en su propio hilo.
    Abre su navegador independiente, se loguea y procesa su sublista.
    """
    logger.info(f"👷 [Worker-{id_worker}] Iniciando sesión Selenium...")
    
    options = Options()
    # options.add_argument("--headless=new")
    # options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    prefs = {
        "profile.managed_default_content_settings.images": 2, # 2 = Bloquear
        "profile.default_content_setting_values.notifications": 2, # Bloquear notificaciones
        "profile.managed_default_content_settings.stylesheets": 2, # A veces rompe sitios, probar con cuidado (opcional)
    }
    options.add_experimental_option("prefs", prefs)
    options.page_load_strategy = 'eager'
    
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)
    
    lista_worker_enriquecida = []

    try:
        # 1. LOGIN
        logger.info(f"🔐 [Worker-{id_worker}] Logueando en HousePricing...")
        if cancel_event.is_set(): 
            driver.quit(); return []

        driver.get(LOGIN_URL)
        logger.debug(f"   ➡️ Navegando a URL de login...")
        wait.until(EC.presence_of_element_located((By.ID, "id_email"))).send_keys(EMAIL)
        logger.debug(f"   ➡️ Ingresando correo...")
        driver.find_element(By.ID, "id_password").send_keys(PASSWORD)
        logger.debug(f"   ➡️ Ingresando contraseña...")

        driver.execute_script("arguments[0].click();", driver.find_element(By.ID, "hp-login-btn"))
        logger.debug(f"   ➡️ Clickando botón de login...")
        wait.until(lambda d: "/login" not in d.current_url)
        logger.success(f"✅ [Worker-{id_worker}] Login exitoso.")
        logger.debug(f"   ➡️ Esperando que se refrezque la página...")
        time.sleep(2)

        # 2. ITERACIÓN
        for i, item in enumerate(sublista_propiedades):
            if cancel_event.is_set():
                logger.info(f"🛑 [Worker-{id_worker}] Proceso cancelado.")
                break
            
            id_prop = item.get("ID_Propiedad")    
            rol = item.get("informacion_general", {}).get("rol")
            comuna = item.get("informacion_general", {}).get("comuna")
            tipo = item.get("caracteristicas",{}).get("Tipo")
            
            logger.info(f"🏁 [Worker-{id_worker}] Procesando: {comuna} | Rol: {rol}")

            if not rol or not comuna:
                logger.warning(f"⏩ [Worker-{id_worker}] Saltando item sin datos.")
                lista_worker_enriquecida.append(item) 
                if callback_progreso: callback_progreso()
                continue
            
            resultado_hp = {
                "lat_centro": None, "lng_centro": None, 
                "resultados": [], 
                "mensaje": "Iniciando"
            }
            MAX_INTENTOS = 3
            exito_rol = False

            for intento in range(MAX_INTENTOS):
                if cancel_event.is_set(): break
                try:
                    # Intento de búsqueda
                    resultado_hp = _buscar_propiedad_individual(driver, wait, comuna, tipo, rol, cancel_event)
                    mensaje = resultado_hp.get("mensaje", "")
                    
                    if "Error" in mensaje or "Timeout" in mensaje:
                        raise Exception(mensaje) # Forzamos el salto al 'except' para reintentar
                    
                    if "Sin resultados" in mensaje:
                        exito_rol = True
                        break
                    if not resultado_hp.get("resultados") and mensaje == "OK":
                        raise Exception("Extracción sin resultados y sin mensaje de validación. Forzando reintento.")
                    
                    exito_rol = True
                    break

                
                except Exception as e:
                    # Capturamos TimeoutException y otros errores de red
                    if intento < MAX_INTENTOS - 1:
                        logger.warning(f"🔄 [Worker-{id_worker}] Fallo intento {intento+1}/{MAX_INTENTOS} para {rol}. Reintentando en 5s... (Error: {e})")
                        time.sleep(3) # Espera de enfriamiento
                        try: 
                            driver.refresh() # Refrescar por si acaso
                            time.sleep(2)
                        except: 
                            pass
                    else:
                        logger.error(f"💀 [Worker-{id_worker}] Fallo definitivo para {rol} tras {MAX_INTENTOS} intentos. Motivo final: {e}")
                        resultado_hp["mensaje"] = f"Error Técnico Persistente: {str(e)}"
                        exito_rol = False
            if not exito_rol:
                # Inyectamos esta bandera para que main_hp.py la intercepte y lo saque de la BD
                item["FATAL_ERROR_DATA"] = True
                item["motivo_error"] = resultado_hp["mensaje"]
                logger.warning(f"🚫 [Worker-{id_worker}] Rol {rol} marcado con FATAL_ERROR_DATA. Será excluido de la BD.")

            valor_comparables = resultado_hp.get("resultados", [])
            if not valor_comparables:
                valor_comparables = resultado_hp.get("mensaje", "Sin resultados")

            item["house_pricing"] = {
                "centro_mapa": {
                    "lat": resultado_hp.get("lat_centro"),
                    "lng": resultado_hp.get("lng_centro")
                },
                "comparables": valor_comparables
            }
            
            lista_worker_enriquecida.append(item)
            if callback_progreso: 
                callback_progreso()
            time.sleep(1)

    except Exception as e:
        logger.error(f"💀 [Worker-{id_worker}] Error crítico: {e}", exc_info=True)
    finally:
        driver.quit()
        logger.info(f"👋 [Worker-{id_worker}] Sesión cerrada.")

    return lista_worker_enriquecida

# ==============================================================================
# ORQUESTADOR PRINCIPAL (ESTANDARIZADO CON PASO 0)
# ==============================================================================
# Modificación: Agregar callback_progreso=None
def procesar_lista_propiedades(lista_propiedades, cancel_event, callback_progreso=None):
    """
    Orquestador que divide la lista y lanza workers en paralelo.
    """
    total = len(lista_propiedades)
    if total == 0: return []
    
    logger.info(f"🚀 Iniciando orquestador Selenium PARALELO para {total} propiedades. WORKERS={WORKERS}")
    
    # División en chunks (igual que Paso 0)
    chunk_size = math.ceil(total / WORKERS)
    chunks = [lista_propiedades[i:i + chunk_size] for i in range(0, total, chunk_size)]
    
    lista_final_consolidada = []
    procesados_count = 0
    lock_progreso = threading.Lock()

    def incrementar_progreso():
        nonlocal procesados_count
        with lock_progreso:
            procesados_count += 1
            if callback_progreso:
                callback_progreso(procesados_count, total)


    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            # Pasamos la función incrementar_progreso a cada worker
            futures.append(executor.submit(procesar_lote_worker, i+1, chunk, cancel_event, incrementar_progreso))
        
        # Recolección de resultados
        for future in as_completed(futures):
            try:
                res_worker = future.result()
                lista_final_consolidada.extend(res_worker)
            except Exception as e:
                logger.error(f"❌ Error en worker: {e}")
            
    logger.success(f"🏁 Proceso finalizado. Propiedades procesadas: {len(lista_final_consolidada)}/{total}")     
    return lista_final_consolidada