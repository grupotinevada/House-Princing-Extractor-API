############################################################################################################################
#  El paso 1 (VERSIÓN HTML) extrae la información del informe web.
#  Es más rápido, exacto y no consume memoria como el PDF.
############################################################################################################################

import os
import re
import uuid
import json
from typing import Dict, Any, List
from bs4 import BeautifulSoup

from logger import get_logger

logger = get_logger("paso1_html_hp", log_dir="logs", log_file="paso1_html_hp.log")

# --- Helpers de Limpieza ---
def clean_money(text: str) -> int:
    if not text: return 0
    clean = re.sub(r'[^\d]', '', str(text))
    return int(clean) if clean else 0

def clean_float(text: str) -> float:
    if not text: return 0.0
    text_orig = str(text).replace(',', '.')
    clean = re.sub(r'[^\d\.]', '', text_orig)
    try:
        return float(clean)
    except:
        return 0.0

def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip()


# --- LÓGICA DE EXTRACCIÓN HTML ---
def parse_house_pricing_html(html_text: str) -> Dict[str, Any]:
    logger.debug("🧠 Iniciando parseo de HTML con BeautifulSoup...")
    
    data = {
        "ID_Propiedad": str(uuid.uuid4()), 
        "informacion_general": {},
        "caracteristicas": {},
        "avaluo": { 
            "Avalúo Total": 0,
            "Avalúo Exento": 0,
            "Avalúo Afecto": 0,
            "Contribuciones Semestrales": 0
        },
        "roles_cbr": [], # El HTML actual no trae esto detallado, lo dejamos vacío por compatibilidad
        "deudas": [],
        "construcciones": [],
        "transaccion": {},
        "informacion_cbr": {},
        "raw_text_debug": "Extraido via HTML DOM" 
    }

    soup = BeautifulSoup(html_text, 'html.parser')

    # 1. SECCIÓN RESUMEN
    sec_resumen = soup.find(id="section-resumen")
    if sec_resumen:
        h1 = sec_resumen.find("h1")
        if h1: data["informacion_general"]["direccion"] = clean_text(h1.get_text())
        
        # Iterar sobre las cajas de datos (Comuna, Rol, Propietario)
        cajas = sec_resumen.find_all("div", class_="grid")
        if cajas:
            for div in cajas[0].find_all("div", recursive=False):
                ps = div.find_all("p")
                if len(ps) >= 2:
                    k = clean_text(ps[0].get_text()).lower()
                    v = clean_text(ps[1].get_text())
                    if "comuna" in k: data["informacion_general"]["comuna"] = v
                    elif "rol" in k: data["informacion_general"]["rol"] = v
                    elif "propietario" in k: data["informacion_general"]["propietario"] = v

    # 2. SECCIÓN CARACTERÍSTICAS Y AVALÚO
    sec_carac = soup.find(id="section-caracteristicas")
    if sec_carac:
        filas = sec_carac.find_all("div", class_="flex justify-between border-b pb-2")
        for fila in filas:
            spans = fila.find_all("span")
            if len(spans) >= 2:
                k = clean_text(spans[0].get_text())
                v = clean_text(spans[1].get_text())
                
                # Clasificar si va a caracteristicas o avaluo
                if k in ["Tipo", "Destino", "Estacionamientos", "Bodegas"]:
                    data["caracteristicas"][k] = v
                elif "M²" in k:
                    data["caracteristicas"][k.replace("M²", "M2")] = clean_float(v)
                elif "Avalúo" in k or "Contribuciones" in k:
                    data["avaluo"][k] = clean_money(v)

    # 3. SECCIÓN DEUDA
    sec_deuda = soup.find(id="section-deuda")
    if sec_deuda:
        filas_deuda = sec_deuda.find_all("div", class_="border-b pb-4")
        for fila in filas_deuda:
            spans = fila.find_all("span")
            a_tag = fila.find("a", href=True)
            
            if len(spans) >= 2:
                rol_str = clean_text(spans[0].get_text())
                monto = clean_money(spans[1].get_text())
                link = a_tag["href"] if a_tag else None
                
                data["deudas"].append({
                    "rol": rol_str,
                    "monto": monto,
                    "link_tgr": link
                })

    # 4. SECCIÓN CONSTRUCCIONES
    sec_const = soup.find(id="section-construcciones")
    if sec_const:
        tbody = sec_const.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 7:
                    data["construcciones"].append({
                        "nro": clean_text(tds[0].get_text()),
                        "material": clean_text(tds[1].get_text()),
                        "calidad": clean_text(tds[2].get_text()),
                        "condicion": clean_text(tds[3].get_text()),
                        "anio": clean_text(tds[4].get_text()),
                        "m2": clean_float(tds[5].get_text()),
                        "destino": clean_text(tds[6].get_text())
                    })

    # 5. SECCIÓN TRANSACCIÓN CBR
    sec_cbr = soup.find(id="section-transaccion-cbr")
    if sec_cbr:
        # Extraer pares clave-valor (Monto, Foja, etc)
        filas_cbr = sec_cbr.find_all("div", class_="flex justify-between border-b pb-2")
        for fila in filas_cbr:
            spans = fila.find_all("span")
            if len(spans) >= 2:
                k = clean_text(spans[0].get_text())
                v = clean_text(spans[1].get_text())
                
                if k == "Monto":
                    data["transaccion"]["monto"] = v
                elif k == "Fecha SII":
                    data["transaccion"]["fecha"] = v
                elif k in ["Foja", "Número", "Año CBR", "Acto"]:
                    # Ajuste de key para mantener retrocompatibilidad con el parser PDF
                    k_limpio = "Año" if k == "Año CBR" else k
                    data["informacion_cbr"][k_limpio] = v

        # Extraer listas (Compradores y Vendedores)
        for h3 in sec_cbr.find_all("h3"):
            k = clean_text(h3.get_text()).lower()
            if k in ["compradores", "vendedores"]:
                ul = h3.find_next_sibling("ul")
                if ul:
                    data["transaccion"][k] = [clean_text(li.get_text()) for li in ul.find_all("li")]

    # Limpieza final de valores "Sin Información"
    for lista in ["compradores", "vendedores"]:
        if data["transaccion"].get(lista) and "Sin Información" in data["transaccion"][lista][0]:
            data["transaccion"][lista] = []

    return data


# ==============================================================================
# PROCESAMIENTO EN LOTE HTML
# ==============================================================================
def procesar_lote_htmls(carpeta_entrada: str, cancel_event, callback_progreso=None) -> List[Dict[str, Any]]:
    logger.info(f"🏁 Iniciando proceso de lote HTML en: {carpeta_entrada}")
    resultados_json = []
    
    if not os.path.exists(carpeta_entrada):
        logger.error(f"❌ La carpeta de entrada {carpeta_entrada} NO existe.")
        return []

    # OJO: Ahora filtramos por archivos .html
    archivos = [f for f in os.listdir(carpeta_entrada) if f.lower().endswith(".html")]
    total_archivos = len(archivos)
    logger.info(f"📂 Archivos HTML encontrados: {total_archivos}")

    for idx, archivo in enumerate(archivos):
        if cancel_event.is_set():
            logger.warning("🛑 Proceso cancelado.")
            return []

        if callback_progreso:
            callback_progreso(idx, total_archivos)

        ruta_completa = os.path.join(carpeta_entrada, archivo)
        logger.info(f"👉 [{idx+1}/{total_archivos}] Procesando: {archivo}")
        
        rol_inferido = "Desconocido"
        comuna_inferida = "Desconocida"
        try:
            nombre_sin_ext = archivo.replace(".html", "")
            partes = nombre_sin_ext.split("_")
            if len(partes) >= 2:
                rol_inferido = partes[-1]
                comuna_inferida = "_".join(partes[:-1])
        except:
            pass

        try:
            tamanio = os.path.getsize(ruta_completa)
            if tamanio == 0:
                raise Exception("El HTML descargado está vacío (0 KB).")

            with open(ruta_completa, "r", encoding="utf-8") as f:
                html_text = f.read()

            # Llama al parser HTML
            datos_extraidos = parse_house_pricing_html(html_text)

            if not datos_extraidos["informacion_general"].get("rol"):
                raise Exception("No se detectó el Rol en el HTML. El formato del DOM pudo haber cambiado.")

            # --- LECTURA DE METADATA (TASACIONES DESDE EL PASO 0) ---
            # El paso 0 guarda la data en archivo.pdf.json por compatibilidad. 
            # Reconstruimos la ruta esperada:
            nombre_base_pdf = archivo.replace(".html", ".pdf")
            ruta_meta = os.path.join(carpeta_entrada, f"{nombre_base_pdf}.json")
            
            link_informe_recuperado = None
            tasacion_data = {
                "tasa_vta_clp": 0, "tasa_vta_uf": "0", "tasa_arr_clp": 0, "tasa_arr_uf": "0"
            }

            if os.path.exists(ruta_meta):
                try:
                    with open(ruta_meta, "r", encoding="utf-8") as fm:
                        meta_data = json.load(fm)
                        link_informe_recuperado = meta_data.get("link_informe")
                        tasacion_data["tasa_vta_clp"] = meta_data.get("tasa_vta_clp", 0)
                        tasacion_data["tasa_vta_uf"] = meta_data.get("tasa_vta_uf", "0")
                        tasacion_data["tasa_arr_clp"] = meta_data.get("tasa_arr_clp", 0)
                        tasacion_data["tasa_arr_uf"] = meta_data.get("tasa_arr_uf", "0")
                        logger.debug(f"     ✅ Metadata y Tasación recuperadas exitosamente.")
                except Exception as e:
                    logger.warning(f"   ⚠️ No se pudo leer metadata adjunta: {e}")
            
            datos_extraidos["meta_archivo"] = {
                "nombre": archivo,
                "ruta": ruta_completa,
                "link_informe": link_informe_recuperado 
            }
            
            # Fusionar tasaciones en la raíz
            datos_extraidos.update(tasacion_data)
            
            resultados_json.append(datos_extraidos)
            logger.success(f"✅ ÉXITO: {archivo} (HTML) procesado y datos estructurados.")

        except Exception as e:
            logger.error(f"❌ FATAL: Error leyendo {archivo}. Excepción: {e}")
            resultados_json.append({
                "ID_Propiedad": str(uuid.uuid4()),
                "informacion_general": {"rol": rol_inferido, "comuna": comuna_inferida},
                "FATAL_ERROR_DATA": True,
                "motivo_error": str(e)
            })
            continue

    if callback_progreso:
        callback_progreso(total_archivos, total_archivos)

    logger.success(f"🎉 Proceso HTML finalizado. Total evaluados: {len(resultados_json)}")
    return resultados_json