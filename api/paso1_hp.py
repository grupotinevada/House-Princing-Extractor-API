############################################################################################################################
#  El paso 1 es el que extrae la infromación del pdf (Informa de antecedentes) 
#  Extrae todos la data del pdf y la guarda en un json 
############################################################################################################################

import re
import uuid
import os
import logging
from typing import Dict, Any, List, Optional
import pdfplumber

from logger import get_logger

logger = get_logger("paso1_hp", log_dir="logs", log_file="paso1_hp.log")

# --- Helpers de Limpieza ---
def clean_money(text: str) -> int:
    if not text: return 0
    clean = re.sub(r'[^\d]', '', text)
    val = int(clean) if clean else 0
    # logger.debug(f"Cleaning money: '{text}' -> {val}") # Comentado para no saturar, descomentar si hay dudas de montos
    return val

def clean_float(text: str) -> float:
    if not text: return 0.0
    text_orig = text
    text = text.replace(',', '.')
    clean = re.sub(r'[^\d\.]', '', text)
    try:
        val = float(clean)
        return val
    except:
        logger.warning(f"⚠️ Fallo al convertir float: '{text_orig}' -> retornando 0.0")
        return 0.0

def clean_text(text: str) -> str:
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

# --- NUEVO: Helper de Coordenadas (CORREGIDO "JUSTO DEBAJO") ---
def map_roles_to_links(pdf_obj) -> Dict[str, str]:
    """
    DEBUG VERSION: Recorre las páginas del PDF y mapea: 
    Numero de Rol (string) -> URL del Link (string)
    """
    mapping = {}
    
    # Rango de búsqueda vertical hacia abajo (en puntos PDF)
    # Aumentemos un poco la tolerancia inicial a 35 por seguridad
    Y_SEARCH_DOWN = 35 

    logger.info("--- 📡 INICIANDO MAPEO DE LINKS (COORDENADAS) ---")

    for p_idx, page in enumerate(pdf_obj.pages):
        try:
            logger.debug(f"📄 Procesando página {p_idx + 1} para mapeo de links...")
            
            # 1. Extraer palabras y links
            words = page.extract_words()
            links = page.hyperlinks
            
            logger.info(f"   ↳ Página {p_idx + 1}: {len(links)} hipervínculos detectados | {len(words)} palabras detectadas.")

            # Imprimir todos los links para ver si existen y dónde están
            if links:
                for i, l in enumerate(links):
                    l_top = l['top']
                    l_left = l['x0']
                    uri = l.get('uri', 'Sin URI')
                    # Solo debug profundo
                    # logger.debug(f"    [Link #{i}] Top: {l_top:.2f} | Left: {l_left:.2f} | URI: {uri}")

            # Filtramos solo las palabras que parecen Roles (ej: "9064-112")
            rol_candidates = [w for w in words if re.match(r'^\d+-[\dKk]+$', w['text'])]
            logger.debug(f"   ↳ Candidatos a Rol encontrados en pág {p_idx+1}: {len(rol_candidates)}")

            for word in rol_candidates:
                rol_text = word['text']
                word_bottom = word['bottom'] 
                word_left = word['x0']
                
                # logger.debug(f"🔍 Analizando ROL '{rol_text}' (Bottom: {word_bottom:.2f}, Left: {word_left:.2f})")
                
                # Buscamos links candidatos
                candidates = []
                for link in links:
                    link_top = link['top']
                    link_left = link['x0']
                    
                    # CÁLCULOS
                    dist_vertical = link_top - word_bottom
                    dist_horizontal = abs(link_left - word_left)
                    
                    # LOG DE COMPARACIÓN
                    # Solo logueamos si está "cerca" para no ensuciar tanto
                    if -10 <= dist_vertical <= 100: 
                        pass
                        # logger.debug(f"     -> vs Link ({link.get('uri')[:15]}...): Vert={dist_vertical:.2f}, Horiz={dist_horizontal:.2f}")

                    # CONDICIÓN 1: Vertical (Debe estar debajo, pero cerca)
                    # Permitimos un pequeño margen negativo (-2) por si se solapan ligeramente
                    is_below = -2 <= dist_vertical <= Y_SEARCH_DOWN
                    
                    # CONDICIÓN 2: Horizontal (Alineación)
                    # Aumentamos tolerancia a 150 por si el link está centrado respecto a la columna y el rol alineado a la izquierda
                    is_aligned = dist_horizontal < 150
                    
                    if is_below and is_aligned:
                        candidates.append(link)
                        logger.debug(f"     ✅ MATCH POTENCIAL para {rol_text}: Distancia Vert: {dist_vertical:.2f}")
                
                # Si hay candidatos, elegimos el más cercano verticalmente
                if candidates:
                    candidates.sort(key=lambda x: x['top'])
                    selected_link = candidates[0]['uri']
                    mapping[rol_text] = selected_link
                    logger.success(f"   🎯 LINK ASIGNADO A {rol_text} -> {selected_link}")
                else:
                    logger.debug(f"     ⚠️ Sin link cercano para {rol_text} (Revisar tolerancias)")
        
        except Exception as e:
            logger.error(f"❌ Error mapeando links en página {p_idx + 1}: {e}")
                    
    logger.info(f"--- FIN MAPEO LINKS: {len(mapping)} roles mapeados ---")
    return mapping

# --- LÓGICA DE EXTRACCIÓN PRINCIPAL ---
def parse_house_pricing_text(full_text: str, link_map: Dict[str, str] = {}) -> Dict[str, Any]:
    logger.debug("🧠 Iniciando parseo de texto plano...")
    
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
        "roles_cbr": [],
        "deudas": [],
        "construcciones": [],
        "transaccion": {},
        "informacion_cbr": {},
        "raw_text_debug": "" 
    }

    lines = full_text.split('\n')
    logger.debug(f"   ↳ Total líneas extraídas: {len(lines)}")
    
    # --- 1. Extracción Estructural (Línea por Línea) ---
    for i, line in enumerate(lines):
        line_clean = line.strip()
        
        # Comuna y Rol
        if "Comuna" in line and "Rol" in line:
            if i + 1 < len(lines):
                next_line = lines[i+1].strip()
                parts = re.split(r'\s{2,}', next_line)
                for part in parts:
                    part = part.strip()
                    if re.match(r'^\d+-[\dKk]+$', part):
                        data["informacion_general"]["rol"] = part
                        logger.info(f"   🏠 Rol Propiedad Detectado: {part}")
                    elif len(part) > 2 and not re.match(r'^\d+$', part):
                        data["informacion_general"]["comuna"] = part
                        logger.debug(f"   📍 Comuna Detectada: {part}")

        # Propietario
        if line_clean == "Propietario":
            if i + 1 < len(lines):
                val = lines[i+1].strip()
                if val:
                    data["informacion_general"]["propietario"] = val
                    logger.debug(f"   👤 Propietario: {val}")
        
        # Dirección (Lógica de búsqueda por proximidad)
        if "informe" in line.lower() and "antecedentes" in line.lower():
            for offset in range(1, 10): 
                if i + offset >= len(lines): break
                candidate = lines[i+offset].strip()
                if not candidate: continue
                if any(kw in candidate for kw in ["Comuna", "Rol", "Propietario"]): break
                if len(candidate) > 5 and re.search(r'\d', candidate):
                    data["informacion_general"]["direccion"] = candidate
                    logger.debug(f"   🗺️ Dirección encontrada: {candidate}")
                    break
                    
        # Roles CBR
        if "Roles inscritos en CBR" in line:
            logger.debug("   🔍 Analizando sección Roles CBR...")
            rol_line_index = -1
            for offset in range(1, 6): 
                if i + offset < len(lines):
                    if "ROL" in lines[i+offset].upper() and re.search(r'\d', lines[i+offset]):
                        rol_line_index = i + offset
                        break
            
            if rol_line_index != -1:
                rol_line = lines[rol_line_index]
                type_line = ""
                # Buscar la línea de tipos (Bodega, Estacionamiento) debajo de los roles
                for offset_type in range(1, 5):
                    if rol_line_index + offset_type < len(lines):
                        candidate = lines[rol_line_index + offset_type]
                        if candidate.strip():
                            type_line = candidate
                            break
                
                roles_parts = re.split(r'\s{2,}', rol_line.strip())
                types_parts = re.split(r'\s{2,}', type_line.strip()) if type_line else []
                valid_roles = [r.strip() for r in roles_parts if "ROL" in r.upper()]
                valid_types = [t.strip() for t in types_parts if t.strip()]

                for idx, rol_val in enumerate(valid_roles):
                    t_val = valid_types[idx] if idx < len(valid_types) else "S/I"
                    data["roles_cbr"].append({
                        "rol": rol_val,
                        "tipo": t_val
                    })
                logger.info(f"   📚 Roles CBR extraídos: {len(data['roles_cbr'])}")

        # Construcciones
        match_cons = re.search(r'^(\d+)\s+(.+?)\s+(20\d{2})\s+([\d,.]+)\s+(.+)$', line.strip())
        if match_cons:
            try:
                const_obj = {
                    "nro": match_cons.group(1),
                    "material": match_cons.group(2).strip(), 
                    "calidad": "", 
                    "anio": match_cons.group(3),
                    "m2": clean_float(match_cons.group(4)),
                    "destino": match_cons.group(5).strip()
                }
                data["construcciones"].append(const_obj)
                # logger.debug(f"   🏗️ Construcción detectada: {const_obj['anio']} - {const_obj['m2']}m2")
            except Exception as e:
                logger.warning(f"⚠️ Error parseando línea construcción '{line.strip()}': {e}")
                pass 

    # --- 2. Regex Globales ---

    # Características
    logger.debug("   ⚙️ Ejecutando Regex Globales (Características)...")
    patterns_carac = {
        "Tipo": r'Tipo\s+([A-Za-z\s]+?)(?=\n|$)', 
        "Destino": r'Destino\s+([A-Za-z\s]+?)(?=\n|$)',
        "M2 Construcción": r'M² Construcción\s+([\d,]+)',
        "M2 Terreno": r'M² Terreno\s+([\d,]+)',
        "Estacionamientos": r'Estacionamientos\s+(.+?)(?=\n|$)',
        "Bodegas": r'Bodegas\s+(.+?)(?=\n|$)'
    }
    
    for key, pat in patterns_carac.items():
        match = re.search(pat, full_text)
        if match:
            val = match.group(1).strip()
            if "M2" in key:
                data["caracteristicas"][key] = clean_float(val)
            else:
                data["caracteristicas"][key] = clean_text(val)
    
    # Ajuste M2
    if data["caracteristicas"].get("M2 Terreno", 0.0) == 0.0:
        logger.debug("   ℹ️ M2 Terreno no detectado, usando M2 Construcción como fallback.")
        data["caracteristicas"]["M2 Terreno"] = data["caracteristicas"].get("M2 Construcción", 0.0)

    # Avalúo
    patterns_avaluo = {
        "Avalúo Total": r'Avalúo Total\s+(\$[\d\.]+)',
        "Avalúo Exento": r'Avalúo Exento\s+(\$[\d\.]+)',
        "Avalúo Afecto": r'Avalúo Afecto\s+(\$[\d\.]+)',
        "Contribuciones Semestrales": r'Contribuciones Semestrales\s+(\$[\d\.]+)'
    }
    for key, pattern in patterns_avaluo.items():
        match = re.search(pattern, full_text)
        if match:
            data["avaluo"][key] = clean_money(match.group(1))

    # --- 3. DEUDAS (Integración con Mapa Espacial) ---
    logger.debug("   💰 Buscando Deudas y cruzando con Link Map...")
    # Buscamos patrones de texto "Rol X $Monto"
    deuda_matches = re.findall(r'(Rol\s+(\d+-[\dKk]+))\s+(\$[\d\.]+)', full_text, re.IGNORECASE)
    seen_deudas = set()
    
    for rol_full_str, rol_number, monto_str in deuda_matches:
        if rol_full_str not in seen_deudas: 
            
            # Buscamos si existe un link detectado debajo de este Rol en el mapa espacial
            found_link = link_map.get(rol_number, None)
            
            data["deudas"].append({
                "rol": rol_full_str,
                "monto": clean_money(monto_str),
                "link_tgr": found_link  # <--- Asignamos el link encontrado
            })
            seen_deudas.add(rol_full_str)
            if found_link:
                logger.success(f"     ✅ Deuda asociada a link: {rol_number} -> {monto_str}")
            else:
                logger.warning(f"     ⚠️ Deuda sin link encontrado: {rol_number}")

    # Transacción
    match_monto = re.search(r'Monto\s+(UF\s+[\d\.]+)', full_text)
    match_fecha = re.search(r'Fecha SII\s+(\d{2}/\d{2}/\d{4})', full_text)
    
    if match_monto: data["transaccion"]["monto"] = match_monto.group(1)
    if match_fecha: data["transaccion"]["fecha"] = match_fecha.group(1)
    
    # Compradores/Vendedores (Lógica defensiva)
    try:
        if "Compradores" in full_text and "Vendedores" in full_text:
            bloque = full_text.split("Compradores")[1].split("Información CBR")[0]
            partes = bloque.split("Vendedores")
            comps = partes[0].strip().split('\n')
            vends = partes[1].strip().split('\n') if len(partes) > 1 else []
            data["transaccion"]["compradores"] = [c.replace('•', '').strip() for c in comps if c.strip()]
            data["transaccion"]["vendedores"] = [v.replace('•', '').strip() for v in vends if v.strip()]
            logger.debug(f"   👥 Transacción: {len(data['transaccion']['compradores'])} compradores / {len(data['transaccion']['vendedores'])} vendedores.")
    except Exception as e:
        logger.error(f"❌ Error parseando compradores/vendedores: {e}")
        pass

    # Información CBR
    patterns_cbr = {
        "Foja": r'Foja\s+(.+?)(?=\n|$)',
        "Número": r'Número\s+(.+?)(?=\n|$)',
        "Año": r'Año CBR\s+(.+?)(?=\n|$)',
        "Acto": r'Acto\s+(.+?)(?=\n|$)'
    }
    
    for key, pat in patterns_cbr.items():
        match = re.search(pat, full_text)
        if match:
            data["informacion_cbr"][key] = match.group(1).strip()

    return data

# --- PROCESAMIENTO EN LOTE ---
# Modificación: Agregar callback_progreso=None
def procesar_lote_pdfs(carpeta_entrada: str, cancel_event, callback_progreso=None) -> List[Dict[str, Any]]:
    logger.info(f"🏁 Iniciando proceso de lote en: {carpeta_entrada}")
    resultados_json = []
    
    if not os.path.exists(carpeta_entrada):
        logger.error(f"❌ La carpeta de entrada {carpeta_entrada} NO existe.")
        return []

    archivos = [f for f in os.listdir(carpeta_entrada) if f.lower().endswith(".pdf")]
    total_archivos = len(archivos)
    logger.info(f"📂 Archivos encontrados: {total_archivos}")

    for idx, archivo in enumerate(archivos):
        if cancel_event.is_set():
            logger.warning("🛑 Proceso cancelado por el usuario (Evento Cancel Set).")
            return []
        
        # --- NUEVO: Actualización de progreso ---
        if callback_progreso:
            callback_progreso(idx, total_archivos)
        # ----------------------------------------

        ruta_completa = os.path.join(carpeta_entrada, archivo)
        logger.info(f"👉 [{idx+1}/{total_archivos}] Procesando: {archivo}")
        
        try:
            full_text = ""
            link_mapping = {}

            with pdfplumber.open(ruta_completa) as pdf:
                logger.debug(f"   📖 Abierto {archivo} con pdfplumber ({len(pdf.pages)} páginas)")
                
                # 1. Mapeo de Links (Coordenadas)
                # Buscamos links DEBAJO de los roles antes de extraer el texto plano
                link_mapping = map_roles_to_links(pdf)
                
                # 2. Extracción de Texto Completo
                for p_idx, page in enumerate(pdf.pages):
                    text = page.extract_text(layout=True)
                    if text: full_text += text + "\n"
                    # logger.debug(f"   📄 Texto extraído pág {p_idx+1}")

            # 3. Parsing
            datos_extraidos = parse_house_pricing_text(full_text, link_map=link_mapping)
            
            datos_extraidos["meta_archivo"] = {
                "nombre": archivo,
                "ruta": ruta_completa
            }
            
            if datos_extraidos["informacion_general"].get("rol"):
                resultados_json.append(datos_extraidos)
                logger.success(f"✅ ÉXITO: {archivo} procesado y datos estructurados.")
            else:
                logger.warning(f"⚠️ DATOS PARCIALES: {archivo} procesado pero no se detectó ROL principal.")

        except Exception as e:
            logger.error(f"❌ FATAL: Error leyendo {archivo}. Excepción: {e}", exc_info=True)
            continue

    # Reportar 100% local al finalizar el bucle
    if callback_progreso:
        callback_progreso(total_archivos, total_archivos)

    logger.success(f"🎉 Proceso de lote finalizado. Total extraídos: {len(resultados_json)}")
    return resultados_json