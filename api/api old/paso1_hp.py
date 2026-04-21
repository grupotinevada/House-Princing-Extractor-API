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
import json 

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

def limpiar_dato_si(text: str) -> Any:
    """
    Limpia basura del PDF (LaTeX/OCR) y maneja el S/I.
    Ej: "$1(S/l)$" -> 1 (int)
    Ej: "S/I" -> None
    """
    if not text: return None
    
    # 1. Limpieza de basura visual ($ y backslash) y espacios
    s = str(text).replace('$', '').replace('\\', '').strip()
    
    # 2. Regex para borrar "S/I", "S/l", "(S/I)" ignorando mayúsculas y variantes OCR
    s = re.sub(r'\(?S/[Il1\|]\)?', '', s, flags=re.IGNORECASE).strip()
    
    # 3. Si quedó vacío (era solo S/I), retornamos None
    if not s:
        return None
        
    # 4. Intentamos convertir a número si quedó algo (ej: "1")
    try:
        # Reemplazar coma por punto por si viene decimal
        s_num = s.replace(',', '.')
        if '.' in s_num:
            return float(s_num)
        return int(s_num)
    except:
        # Si no es número (ej: "Habitacional"), devolvemos el texto limpio
        return s

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

# --- EXTRACCIÓN ESPACIAL DE CONSTRUCCIONES ---
def extraer_construcciones_espacial(pdf_obj) -> List[Dict[str, Any]]:
    """
    Extrae el 'Detalle Construcciones' usando coordenadas espaciales de palabras (pdfplumber).

    El PDF genera celdas multi-línea (ej: material en 2 líneas, calidad en 2 líneas)
    que no pueden capturarse con un regex de línea simple. Esta función:
    1. Localiza el encabezado buscando la celda "N°" y calcula rangos X de columnas.
    2. Agrupa las palabras de datos en bandas horizontales (líneas).
    3. Detecta saltos verticales grandes entre bandas para separar registros.
    4. Fusiona todas las bandas de cada bloque en un registro completo.
    """
    construcciones = []
    Y_TOLERANCE   = 4    # puntos de tolerancia para agrupar palabras en la misma línea
    FOOTER_MARGIN = 80   # ignorar pie de página (últimos N puntos de la página)
    ROW_GAP       = 15   # brecha vertical mínima entre bandas para separar registros

    for page in pdf_obj.pages:
        page_height = page.height
        words = page.extract_words(x_tolerance=3, y_tolerance=3, keep_blank_chars=False)
        if not words:
            continue

        # 1. Localizar encabezado por la celda "N°"
        header_words = [w for w in words if w['text'].strip() == "N°"]
        if not header_words:
            continue
        header_y = header_words[0]['top']

        # 2. Calcular rangos X de cada columna a partir del encabezado
        header_row = sorted(
            [w for w in words if abs(w['top'] - header_y) <= Y_TOLERANCE],
            key=lambda w: w['x0']
        )
        col_ranges = []
        for i, hw in enumerate(header_row):
            x_end = header_row[i+1]['x0'] if i+1 < len(header_row) else float('inf')
            col_ranges.append({'name': hw['text'], 'x0': hw['x0'], 'x1': x_end})

        logger.debug(f"   🏗️ Columnas construcciones detectadas: {[c['name'] for c in col_ranges]}")

        # 3. Palabras de datos: debajo del encabezado y por encima del pie de página
        cutoff_y = page_height - FOOTER_MARGIN
        data_words = [
            w for w in words
            if w['top'] > header_y + Y_TOLERANCE and w['top'] < cutoff_y
        ]

        # 4. Agrupar palabras en bandas horizontales (líneas)
        bands = []
        for w in sorted(data_words, key=lambda x: x['top']):
            placed = False
            for band in bands:
                if abs(w['top'] - band['y']) <= Y_TOLERANCE:
                    band['words'].append(w)
                    placed = True
                    break
            if not placed:
                bands.append({'y': w['top'], 'words': [w]})

        def assign_cols(band):
            row = {c['name']: [] for c in col_ranges}
            for w in band['words']:
                for col in col_ranges:
                    if col['x0'] <= w['x0'] < col['x1']:
                        row[col['name']].append(w['text'])
                        break
            return row

        rows = [assign_cols(b) for b in bands]
        if not rows:
            continue

        # 5. Agrupar bandas en bloques usando saltos verticales grandes
        #    Cada bloque corresponde a un registro de construcción
        blocks = []
        current_block = [rows[0]]
        for j in range(1, len(rows)):
            gap = bands[j]['y'] - bands[j-1]['y']
            if gap > ROW_GAP:
                blocks.append(current_block)
                current_block = [rows[j]]
            else:
                current_block.append(rows[j])
        blocks.append(current_block)

        # 6. Procesar cada bloque como un registro
        for block in blocks:
            # Solo procesar bloques que contengan un N° numérico válido
            has_nro = any(
                ' '.join(r.get('N°', [])).strip().isdigit()
                for r in block
            )
            if not has_nro:
                continue

            # Fusionar todas las palabras del bloque por columna
            merged = {c['name']: [] for c in col_ranges}
            for row in block:
                for col in col_ranges:
                    merged[col['name']].extend(row[col['name']])

            def dedup_ordered(text):
                """Elimina duplicados manteniendo orden (para casos como 'Media inferior Media inferior')."""
                ws = text.split()
                seen, out = set(), []
                for w in ws:
                    if w.lower() not in seen:
                        seen.add(w.lower())
                        out.append(w)
                return ' '.join(out)

            obj = {
                "nro":       ' '.join(merged.get('N°', [])).strip(),
                "material":  ' '.join(merged.get('Material', [])).strip(),
                "calidad":   dedup_ordered(' '.join(merged.get('Calidad', []))),
                "condicion": ' '.join(merged.get('Condición', [])).strip(),
                "anio":      ' '.join(merged.get('Año', [])).strip(),
                "m2":        clean_float(' '.join(merged.get('M²', []))),
                "destino":   ' '.join(merged.get('Destino', [])).strip(),
            }
            construcciones.append(obj)
            logger.debug(f"   🏗️ Construcción detectada: N°{obj['nro']} {obj['material']} {obj['m2']}m2 ({obj['anio']})")

    logger.info(f"   🏗️ Total construcciones extraídas: {len(construcciones)}")
    return construcciones


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
                    tipo_limpio = limpiar_dato_si(t_val)
                    data["roles_cbr"].append({
                        "rol": rol_val,
                        "tipo": tipo_limpio
                    })
                logger.info(f"   📚 Roles CBR extraídos: {len(data['roles_cbr'])}")

        pass  # Construcciones se extraen espacialmente en extraer_construcciones_espacial()

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
                data["caracteristicas"][key] = limpiar_dato_si(val)
    
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
            val = match.group(1).strip()
            if key == "Año":
                # Limpieza específica para año S/I
                dato_limpio = limpiar_dato_si(val)
                # Si limpiar_dato_si nos devuelve None (porque decía "S/I" o estaba vacío), aplicamos la nueva regla
                data["informacion_cbr"][key] = dato_limpio if dato_limpio is not None else "Sin datos desde hp"
            else:
                data["informacion_cbr"][key] = val  
        else:
            # Si la sección entera no existe en el PDF (como en San Fernando), ponemos la etiqueta
            data["informacion_cbr"][key] = "Sin datos desde hp"

    return data


# ==============================================================================
# PROCESAMIENTO EN LOTE 
# ==============================================================================
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

        if callback_progreso:
            callback_progreso(idx, total_archivos)

        ruta_completa = os.path.join(carpeta_entrada, archivo)
        logger.info(f"👉 [{idx+1}/{total_archivos}] Procesando: {archivo}")
        
        # --- NUEVA SEMÁNTICA: Inferir datos por si el archivo está roto ---
        rol_inferido = "Desconocido"
        comuna_inferida = "Desconocida"
        try:
            nombre_sin_ext = archivo.replace(".pdf", "")
            partes = nombre_sin_ext.split("_")
            if len(partes) >= 2:
                rol_inferido = partes[-1]
                comuna_inferida = "_".join(partes[:-1])
        except:
            pass

        try:
            # --- NUEVA SEMÁNTICA: Validación de archivo corrupto o vacío ---
            tamanio = os.path.getsize(ruta_completa)
            if tamanio == 0:
                raise Exception("El PDF descargado está corrupto, ilegible o vacío (0 KB).")

            full_text = ""
            link_mapping = {}

            # --- NUEVA SEMÁNTICA: Captura de errores internos de la librería ---
            try:
                with pdfplumber.open(ruta_completa) as pdf:
                    logger.debug(f"   📖 Abierto {archivo} con pdfplumber ({len(pdf.pages)} páginas)")
                    
                    # 1. Mapeo de Links (Coordenadas)
                    link_mapping = map_roles_to_links(pdf)
                    
                    # 2. Extracción de Texto Completo
                    construcciones_espaciales = []
                    for p_idx, page in enumerate(pdf.pages):
                        text = page.extract_text(layout=True)
                        if text: full_text += text + "\n"

                    # Extracción espacial de construcciones (más robusta que regex de línea)
                    construcciones_espaciales = extraer_construcciones_espacial(pdf)
            except Exception as e_pdf:
                raise Exception(f"El archivo PDF no tiene un formato válido o está protegido contra lectura. Detalle técnico: {e_pdf}")

            # 3. Parsing
            datos_extraidos = parse_house_pricing_text(full_text, link_map=link_mapping)

            # Inyectar construcciones espaciales (sobreescribe resultado de regex si hay datos)
            if construcciones_espaciales:
                datos_extraidos["construcciones"] = construcciones_espaciales
                logger.info(f"   ✅ Construcciones por extracción espacial: {len(construcciones_espaciales)} registros.")

            # --- NUEVA SEMÁNTICA: Cambio de formato en HP ---
            if not datos_extraidos["informacion_general"].get("rol"):
                raise Exception("No se detectó el Rol en el PDF. Es posible que el formato del documento origen de House Pricing haya cambiado.")

            # --- LECTURA DE METADATA (TASACIONES) ---
            link_informe_recuperado = None
            
            # Variables de tasación por defecto
            tasacion_data = {
                "tasa_vta_clp": 0,
                "tasa_vta_uf": "0",
                "tasa_arr_clp": 0,
                "tasa_arr_uf": "0"
            }

            ruta_meta = ruta_completa + ".json"
            if os.path.exists(ruta_meta):
                try:
                    with open(ruta_meta, "r", encoding="utf-8") as fm:
                        meta_data = json.load(fm)
                        link_cand = meta_data.get("link_informe")
                        rol_origen = str(meta_data.get("rol_origen", "")).strip().upper()
                        rol_pdf = str(datos_extraidos["informacion_general"].get("rol", "")).strip().upper()

                        # VALIDACIÓN: El rol del JSON debe estar contenido o ser igual al del PDF
                        if rol_origen and rol_pdf and (rol_origen == rol_pdf or rol_origen in rol_pdf):
                            link_informe_recuperado = link_cand
                            
                            # --- NUEVO: Extraer datos de tasación del JSON ---
                            tasacion_data["tasa_vta_clp"] = meta_data.get("tasa_vta_clp", 0)
                            tasacion_data["tasa_vta_uf"] = meta_data.get("tasa_vta_uf", "0")
                            tasacion_data["tasa_arr_clp"] = meta_data.get("tasa_arr_clp", 0)
                            tasacion_data["tasa_arr_uf"] = meta_data.get("tasa_arr_uf", "0")
                            
                            logger.debug(f"     ✅ Metadata y Tasación recuperadas para Rol {rol_origen}.")
                        else:
                            logger.warning(f"     ⚠️ ERROR VALIDACIÓN: El JSON dice rol '{rol_origen}' pero el PDF es '{rol_pdf}'. Link y Tasación ignorados.")
                        
                except Exception as e:
                    logger.warning(f"   ⚠️ No se pudo leer metadata adjunta: {e}")
            
            datos_extraidos["meta_archivo"] = {
                "nombre": archivo,
                "ruta": ruta_completa,
                "link_informe": link_informe_recuperado 
            }
            
            # Fusionamos la tasación en la raíz del objeto para que Paso 3 y 4 la encuentren fácil
            datos_extraidos.update(tasacion_data)
            
            resultados_json.append(datos_extraidos)
            logger.success(f"✅ ÉXITO: {archivo} procesado y datos estructurados.")

        except Exception as e:
            logger.error(f"❌ FATAL: Error leyendo {archivo}. Excepción: {e}")
            # --- NUEVA SEMÁNTICA: En vez de un continue vacío, mandamos el error estructurado al Paso 2 ---
            resultados_json.append({
                "ID_Propiedad": str(uuid.uuid4()),
                "informacion_general": {
                    "rol": rol_inferido,
                    "comuna": comuna_inferida
                },
                "FATAL_ERROR_DATA": True,
                "motivo_error": str(e)
            })
            continue

    if callback_progreso:
        callback_progreso(total_archivos, total_archivos)

    logger.success(f"🎉 Proceso de lote finalizado. Total extraídos/evaluados: {len(resultados_json)}")
    return resultados_json