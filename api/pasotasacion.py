import time
import json
import random
import unicodedata
from bs4 import BeautifulSoup
from logger import get_logger

logger = get_logger("pasotasacion", log_dir="logs", log_file="pasotasacion.log")

# ==============================================================================
# DICCIONARIOS DEL BACKEND (EXTRAÍDOS DEL JS ORIGINAL)
# ==============================================================================
from utils import COMUNAS_DATA

# Mapa de Códigos de Propiedad exactos requeridos por el servidor
MAPA_TIPOS = {
    "departamento": "DP",
    "casa": "CA",
    "local comercial": "LC",
    "oficina": "OF",
    "terreno": "TE",
    "estacionamiento": "ES",
    "bodega": "BO"
}

if COMUNAS_DATA and MAPA_TIPOS:
    logger.info(f"mapas cargados correctamente")
else:
    logger.error(f"mapas no cargados correctamente")
    raise Exception("mapas no cargados correctamente")
# Mapa de Comunas -> ID Interno


# Construir diccionario de búsqueda rápida ignorando mayúsculas y acentos
def _normalize_text(text):
    return ''.join((c for c in unicodedata.normalize('NFD', str(text)) if unicodedata.category(c) != 'Mn')).lower()

COMUNAS_DICT = {_normalize_text(c["label"]): c["value"] for c in COMUNAS_DATA}


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def _random_delay(min_s=1.0, max_s=2.5):
    """Previene rate-limiting simulando tiempos de lectura humana."""
    time.sleep(random.uniform(min_s, max_s))

def _clean_clp(txt):
    if not txt: return 0
    clean = txt.replace('$', '').replace('.', '').strip()
    return int(clean) if clean.isdigit() else 0

def _clean_uf(txt):
    if not txt: return "0"
    clean = txt.replace('UF', '').replace('.', '').strip()
    return clean

def _safe_str(val):
    """Retorna un string vacío si es nulo, para simular inputs no enviados."""
    if val is None:
        return ""
    return str(val)

def _obtener_campo(match_data, campo_principal, campo_secundario=None):
    """
    Va a buscar el dato exacto a la rama 'characteristics'.
    Si no existe o no tiene 'value', retorna vacío para no enviar falsos 0.
    """
    ch = match_data.get("characteristics", {})
    if campo_principal in ch and "value" in ch[campo_principal]:
        return ch[campo_principal]["value"]
    
    # Intento secundario si lo requiere
    if campo_secundario and campo_secundario in match_data:
        return match_data[campo_secundario]
        
    return ""


# ==============================================================================
# LÓGICA PRINCIPAL DE TASACIÓN
# ==============================================================================

def obtener_tasacion(session, match_data, csrf_token, url_base="https://www.housepricing.cl", worker_id="N/A", max_intentos=3):
    data_result = {
        "tasa_vta_clp": 0,
        "tasa_vta_uf": "0", 
        "tasa_arr_clp": 0,
        "tasa_arr_uf": "0" 
    }

    url_tasacion_resultado = f"{url_base}/tasacion-resultado/"

    logger.info(f"   💰 [Worker-{worker_id}] Iniciando extracción de tasación (Memoria) para Rol {match_data.get('rol')}...")

    nombre_comuna_limpio = _normalize_text(match_data.get("comuna", ""))
    id_comuna = COMUNAS_DICT.get(nombre_comuna_limpio, "")
    
    tipo_crudo = str(match_data.get("tipo_propiedad", "")).lower().strip()
    property_type_code = MAPA_TIPOS.get(tipo_crudo, "CA")

    # --- REGLA DE NEGOCIO: M2 Total nunca puede ser menor a M2 Util ---
    m2_util_bruto = _obtener_campo(match_data, "m2_util", "m2_util")
    m2_total_bruto = _obtener_campo(match_data, "m2_total", "m2_total")
    
    try:
        val_util = float(m2_util_bruto) if m2_util_bruto not in ("", None) else 0.0
        val_total = float(m2_total_bruto) if m2_total_bruto not in ("", None) else 0.0
        
        if val_total < val_util:
            logger.debug(f"      🔧 [Worker-{worker_id}] Ajustando m2_total ({val_total}) -> ({val_util}) para cumplir validación del servidor.")
            m2_total_bruto = m2_util_bruto 
    except Exception as e:
        logger.warning(f"      ⚠️ [Worker-{worker_id}] No se pudo parsear m2 para validación: {e}")
    # -------------------------------------------------------------------

    payload = {
        "csrfmiddlewaretoken": csrf_token,
        "latitude": _safe_str(match_data.get("latitude")),
        "longitude": _safe_str(match_data.get("longitude")),
        "address": _safe_str(match_data.get("address")),
        "address_comuna": _safe_str(match_data.get("comuna")), 
        "street_number": "",
        "operation_type": "VE",
        "rol_match_res": json.dumps(match_data, separators=(',', ':')), 
        "codigo_sii_comuna": _safe_str(match_data.get("codigo_sii_comuna")),
        "pc_pid": _safe_str(match_data.get("pc_pid")),
        "search-type": "search-rol",
        "comuna": _safe_str(id_comuna), 
        "rol": _safe_str(match_data.get("rol")),
        "property_type": property_type_code,
        "unit": _safe_str(match_data.get("unidad")),
        "year": _safe_str(_obtener_campo(match_data, "year", "year")),
        "m2_util": _safe_str(m2_util_bruto),    # Valor procesado
        "m2_total": _safe_str(m2_total_bruto),  # Valor procesado y blindado
        "bedrooms": _safe_str(_obtener_campo(match_data, "bedrooms")),
        "bathrooms": _safe_str(_obtener_campo(match_data, "bathrooms")),
        "parking": _safe_str(_obtener_campo(match_data, "parking")),
        "storage": _safe_str(_obtener_campo(match_data, "storage")),
        "common_expenses": _safe_str(_obtener_campo(match_data, "common_expenses")),
        "orientation": _safe_str(_obtener_campo(match_data, "orientation")),
        "m2_terrace": _safe_str(_obtener_campo(match_data, "m2_terrace")),
        "floor": _safe_str(_obtener_campo(match_data, "floor")),
        "floors_building": _safe_str(_obtener_campo(match_data, "floors_building")),
    }

    for intento in range(1, max_intentos + 1):
        try:
            _random_delay(1.5, 3.0)
            
            logger.debug(f"      ➡️ [Worker-{worker_id}] (Intento {intento}) Enviando POST a tasacion-resultado...")
            res_post = session.post(
                url_tasacion_resultado, 
                data=payload, 
                headers={
                    "Referer": f"{url_base}/tasacion-de-propiedades/",
                    "Origin": url_base,
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest"
                },
                timeout=15
            )

            if res_post.status_code != 200:
                logger.warning(f"      ⚠️ [Worker-{worker_id}] Error HTTP {res_post.status_code} al generar tasación.")
                continue

            try:
                response_data = res_post.json()
            except json.JSONDecodeError:
                # Modificación permanente para capturar el error exacto del frontend
                soup_error = BeautifulSoup(res_post.text, "html.parser")
                alerta_error = soup_error.find("div", class_=lambda c: c and "bg-red-100" in c)
                
                mensaje_error = "Error desconocido (no se encontró texto de error en el HTML)"
                if alerta_error:
                    mensaje_error = " ".join(alerta_error.stripped_strings)
                
                logger.warning(f"      ⚠️ [Worker-{worker_id}] El servidor rechazó el form. Mensaje del sitio: {mensaje_error}")
                continue
            
            if not response_data.get("success") or "redirect" not in response_data:
                logger.warning(f"      ⚠️ [Worker-{worker_id}] Respuesta JSON sin redirect válido: {response_data}")
                continue

            url_redirect = f"{url_base}{response_data['redirect']}"
            logger.debug(f"      ➡️ [Worker-{worker_id}] Navegando a URL final de tasación...")
            
            _random_delay(1.0, 2.0)
            res_get = session.get(url_redirect, headers={"Referer": f"{url_base}/tasacion-de-propiedades/"}, timeout=15)
            
            if res_get.status_code != 200:
                logger.warning(f"      ⚠️ [Worker-{worker_id}] Error HTTP {res_get.status_code} al cargar HTML final.")
                continue

            soup = BeautifulSoup(res_get.text, "html.parser")
            
            # --- VENTA ---
            h3_venta = soup.find(lambda tag: tag.name == "h3" and "Precio estimado de venta" in tag.text)
            if h3_venta:
                span_clp_vta = h3_venta.find_next("span", class_=lambda c: c and "text-4xl" in c)
                span_uf_vta = h3_venta.find_next("span", string=lambda t: t and "UF" in t)
                data_result["tasa_vta_clp"] = _clean_clp(span_clp_vta.text if span_clp_vta else "")
                data_result["tasa_vta_uf"] = _clean_uf(span_uf_vta.text if span_uf_vta else "")

            # --- ARRIENDO ---
            h3_arriendo = soup.find(lambda tag: tag.name == "h3" and "Precio estimado de arriendo" in tag.text)
            if h3_arriendo:
                span_clp_arr = h3_arriendo.find_next("span", class_=lambda c: c and "text-4xl" in c)
                span_uf_arr = h3_arriendo.find_next("span", string=lambda t: t and "UF" in t)
                data_result["tasa_arr_clp"] = _clean_clp(span_clp_arr.text if span_clp_arr else "")
                data_result["tasa_arr_uf"] = _clean_uf(span_uf_arr.text if span_uf_arr else "")

            logger.success(f"      ✅ [Worker-{worker_id}] Tasación Extraída: Venta ${data_result['tasa_vta_clp']} | Arriendo ${data_result['tasa_arr_clp']}")
            return data_result

        except requests.exceptions.Timeout:
            logger.warning(f"      ⏳ [Worker-{worker_id}] Timeout en la red (Intento {intento}).")
        except Exception as e:
            logger.error(f"      ❌ [Worker-{worker_id}] Error inesperado extrayendo tasación: {str(e)}")
            
    logger.error(f"   ❌ [Worker-{worker_id}] Agotados los {max_intentos} intentos para tasación.")
    return data_result