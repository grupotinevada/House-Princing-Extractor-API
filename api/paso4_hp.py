import mysql.connector
from mysql.connector import Error
import os
import sys
import re  
from typing import List, Dict, Any
from datetime import datetime

# --- AJUSTE DE RUTAS E IMPORTS ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from logger import get_logger
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
logger = get_logger("paso4_hp", log_dir=os.path.join(PROJECT_ROOT, "logs"), log_file="paso4_hp.log")

# ==============================================================================
#  HELPERS / TRADUCTORES DE DATOS (JSON -> MySQL)
# ==============================================================================

def convertir_fecha_mysql(fecha_str: Any) -> Any:
    """Convierte '18/05/2021' o '2021-05-18' a formato MySQL. Retorna None si está vacío."""
    if not fecha_str or str(fecha_str).strip() in ["", "Sin fecha", "None"]:
        return None
    
    fecha_limpia = str(fecha_str).strip()
    
    # Si ya viene en formato de MySQL (YYYY-MM-DD), se retorna directamente
    if re.match(r'^\d{4}-\d{2}-\d{2}$', fecha_limpia):
        return fecha_limpia
        
    # Si viene en formato chileno (DD/MM/YYYY o DD-MM-YYYY), la convertimos
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(fecha_limpia, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass # Pasa al siguiente formato si falla
            
    return None

def limpiar_precio_uf(valor: Any) -> float:
    """
    Normaliza UF/Pesos a float estándar para la BD.
    Ej: "4.638" -> 4638.0 (Quita punto de miles)
    Ej: "17,53" -> 17.53  (Cambia coma por punto decimal)
    """
    if not valor: return 0.0
    
    # 1. Convertir a string y limpiar basura (UF, $, espacios)
    s = str(valor).upper().replace("UF", "").replace("$", "").strip()
    
    # 2. ELIMINAR PUNTO DE MILES (Para que "4.638" sea "4638")
    s = s.replace(".", "")
    
    # 3. REEMPLAZAR COMA POR PUNTO (Para que "17,53" sea "17.53")
    s = s.replace(",", ".")
    
    try:
        return float(s)
    except:
        return 0.0

def limpiar_decimal_chile(valor: Any) -> float:
    """Transforma '94,12' -> 94.12."""
    if not valor: return 0.0
    s = str(valor).replace(",", ".").strip()
    try:
        return float(s)
    except:
        return 0.0

def limpiar_int(valor: Any) -> int:
    """Limpia campos simples como '2 dorms' -> 2."""
    if not valor: return 0
    try:
        s = re.sub(r'[^\d]', '', str(valor))
        return int(s)
    except:
        return 0

def limpiar_SI(texto: Any) -> Any:
    """Convierte a int, retorna None si es 'S/I' o inválido."""
    if not texto or str(texto).strip() in ["", "S/I", "Sin dato", "None", "-"]:
        return None
    try:
        clean = re.sub(r'[^\d]', '', str(texto))
        return int(clean) if clean else None
    except ValueError:
        return None


def limpiar_anio_mysql(valor: Any) -> Any:
    """Convierte a entero para BD. Si viene la etiqueta de error o vacío, retorna None (NULL)."""
    if not valor or str(valor).strip() == "Sin datos desde hp" or str(valor).strip() == "None":
        return None
    try:
        # Extrae solo los números por si viene algo como "2021." o espacios
        s = re.sub(r'[^\d]', '', str(valor))
        return int(s) if s else None
    except:
        return None

# ==============================================================================
#  LÓGICA PRINCIPAL
# ==============================================================================

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        return connection
    except Error as e:
        logger.error(f"❌ Error conectando a MySQL: {e}")
        return None

def insertar_datos(lista_datos: List[Dict[str, Any]], cancel_event, callback_progreso=None) -> bool:
    logger.info("💾 PASO 4: Iniciando inyección a Base de Datos...")
    
    conn = get_db_connection()
    # --- NUEVA SEMÁNTICA: Validación de Conexión Inicial ---
    if not conn: 
        raise Exception("Fallo Crítico: No se pudo establecer conexión con la Base de Datos. Verifique las credenciales o el estado del servidor MySQL.")

    cursor = conn.cursor()
    total = len(lista_datos)
    logger.info(f"📊 Total de propiedades a procesar: {total}")
    
    try:
        for idx, item in enumerate(lista_datos):
            if cancel_event.is_set():
                logger.warning("🛑 Inyección BD cancelada por usuario.")
                conn.rollback()
                return False

            # --- 1. EXTRACCIÓN ---
            uid = item.get("ID_Propiedad")
            gral = item.get("informacion_general", {})
            carac = item.get("caracteristicas", {})
            avaluo = item.get("avaluo", {})
            trans = item.get("transaccion", {})
            cbr = item.get("informacion_cbr", {})
            meta = item.get("meta_archivo", {})
            
            rol_actual = gral.get("rol", "S/R")
            logger.info(f"👉 [{idx+1}/{total}] Procesando UID: {uid} | Rol: {rol_actual}")

            vend_str = ", ".join(trans.get("vendedores", [])) if trans.get("vendedores") else None
            comp_str = ", ".join(trans.get("compradores", [])) if trans.get("compradores") else None
            
            # Debug de datos críticos antes de insertar
            link_debug = meta.get("link_informe")
            logger.debug(f"     🔍 Metadata: Archivo='{meta.get('nombre')}' | Link Informe={'✅ Presente' if link_debug else '❌ NULL'}")

            # --- 2. QUERY  (PROPIEDADES) ---
            
            # Limpiezas críticas
            fecha_tx_clean = convertir_fecha_mysql(trans.get("fecha"))
            monto_tx_clean = limpiar_precio_uf(trans.get("monto"))

            # Se agregan link_informe + las columnas de tasación
            sql_propiedad = """
                INSERT INTO propiedades (
                    id, rol_sii, comuna, direccion, propietario,
                    tipo_propiedad, destino_sii, m2_construido, m2_terreno,
                    avaluo_total, avaluo_exento, avaluo_afecto, contribuciones_semestrales,
                    cbr_foja, cbr_numero, cbr_anio, fecha_transaccion, monto_transaccion,
                    vendedores, compradores, nombre_archivo_origen,
                    link_informe,
                    tasa_vta_clp, tasa_vta_uf, tasa_arr_clp, tasa_arr_uf
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            # Se agrega el valor de link_informe y None para los 4 campos de tasación
            valores_propiedad = (
                uid, gral.get("rol"), gral.get("comuna"), gral.get("direccion"), gral.get("propietario"),
                carac.get("Tipo"), carac.get("Destino"), 
                limpiar_decimal_chile(carac.get("M2 Construcción")), 
                limpiar_decimal_chile(carac.get("M2 Terreno")),
                avaluo.get("Avalúo Total"), avaluo.get("Avalúo Exento"), 
                avaluo.get("Avalúo Afecto"), avaluo.get("Contribuciones Semestrales"),
                limpiar_SI(cbr.get("Foja")), limpiar_SI(cbr.get("Número")), limpiar_anio_mysql(cbr.get("Año")),
                fecha_tx_clean,
                monto_tx_clean, 
                vend_str, comp_str, meta.get("nombre"),
                meta.get("link_informe"), 

                item.get("tasa_vta_clp", 0),
                limpiar_precio_uf(item.get("tasa_vta_uf", "0")), 
                item.get("tasa_arr_clp", 0),
                limpiar_precio_uf(item.get("tasa_arr_uf", "0"))
            )
            
            cursor.execute(sql_propiedad, valores_propiedad)
            logger.debug(f"     ✅ Tabla 'propiedades' insertada correctamente.")

            # --- 3. CONSTRUCCIONES ---
            lista_cons = item.get("construcciones", [])
            if lista_cons:
                logger.debug(f"     🏗️ Insertando {len(lista_cons)} registros en 'construcciones'...")
                for c in lista_cons:
                    sql_cons = """
                        INSERT INTO construcciones (propiedad_id, numero_linea, material, calidad, anio_construccion, m2, destino)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_cons, (
                        uid, c.get("nro"), c.get("material"), c.get("calidad"), 
                        c.get("anio"), 
                        limpiar_decimal_chile(c.get("m2")), 
                        c.get("destino")
                    ))
            else:
                logger.debug("     ⚪ Sin construcciones para insertar.")

            # --- 4. ROLES ASOCIADOS ---
            lista_roles = item.get("roles_cbr", [])
            mapa_deudas = {str(d.get("rol", "")).upper().strip(): d for d in item.get("deudas", [])}
            
            if lista_roles:
                logger.debug(f"     🔗 Insertando {len(lista_roles)} registros en 'roles_asociados'...")
                for r in lista_roles:
                    key = str(r.get("rol", "")).upper().strip()
                    deuda_obj = mapa_deudas.get(key, {})
                    
                    sql_roles = """
                        INSERT INTO roles_asociados (propiedad_id, rol_asociado, tipo_ubicacion, monto_deuda, link_deuda)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_roles, (uid, r.get("rol"), r.get("tipo"), deuda_obj.get("monto", 0), deuda_obj.get("link_tgr")))
            else:
                logger.debug("     ⚪ Sin roles asociados extra.")

            # --- 5. DEUDAS (SOLO ROL PRINCIPAL) ---
            rol_princ_key = str(gral.get("rol", "")).upper().strip()
            deudas_insertadas = 0
            for d in item.get("deudas", []):
                if rol_princ_key in str(d.get("rol", "")).upper().strip():
                    sql_deuda = """
                        INSERT INTO deudas_tgr (propiedad_id, rol_deuda, monto, link_tgr)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(sql_deuda, (uid, d.get("rol"), d.get("monto"), d.get("link_tgr")))
                    deudas_insertadas += 1
            
            if deudas_insertadas > 0:
                logger.debug(f"     💰 Insertada deuda TGR para rol principal.")

            # --- 6. COMPARABLES ---
            hp_data = item.get("house_pricing", {})
            raw_comps = hp_data.get("comparables") 
            
            insertar_placeholder = False
            
            if raw_comps is None:
                logger.warning(f"     ⚠️ [DEBUG] No existe la llave 'comparables' para la propiedad {uid}")
                insertar_placeholder = True
            elif isinstance(raw_comps, str):
                logger.warning(f"     ⚠️ [DEBUG] 'comparables' es un texto (posible error de scraping): {raw_comps}")
                insertar_placeholder = True
            elif isinstance(raw_comps, list):
                if len(raw_comps) == 0:
                    logger.warning(f"     ⚠️ [DEBUG] La lista de comparables está vacía [] para {uid}")
                    insertar_placeholder = True
            else:
                 logger.error(f"     ❌ [DEBUG] Tipo de dato inesperado en 'comparables': {type(raw_comps)}")
                 insertar_placeholder = True

            sql_comp = """
                INSERT INTO comparables (
                    propiedad_id, fuente, rol_comparable, direccion, comuna,
                    precio_uf, uf_m2, fecha_transaccion, fecha_publicacion, anio_construccion,
                    m2_util, m2_total, dormitorios, banios, estacionamientos, bodegas, distancia_metros, link_mapa, link_publicacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            if not insertar_placeholder:
                # INSERCIÓN NORMAL (Hay comparables válidos)
                logger.info(f"     👀 [DEBUG] Se encontraron {len(raw_comps)} comparables. Insertando...")
                for i, comp in enumerate(raw_comps):
                    try:
                        vals_comp = (
                            uid, 
                            comp.get("fuente", "Desconocido"), 
                            comp.get("rol", ""), 
                            comp.get("direccion", ""), 
                            comp.get("comuna", ""),
                            limpiar_precio_uf(comp.get("precio_uf")),
                            limpiar_decimal_chile(comp.get("uf_m2")),
                            convertir_fecha_mysql(comp.get("fecha_transaccion")),
                            convertir_fecha_mysql(comp.get("fecha_publicacion")),
                            comp.get("anio", 0),
                            limpiar_decimal_chile(comp.get("m2_util")), 
                            limpiar_decimal_chile(comp.get("m2_total")), 
                            limpiar_int(comp.get("dormitorios")), 
                            limpiar_int(comp.get("banios")),
                            limpiar_int(comp.get("estacionamientos", 0)),
                            limpiar_int(comp.get("bodegas", 0)),
                            comp.get("distancia_metros", 0),
                            comp.get("link_maps", ""), 
                            comp.get("link_publicacion", "")
                        )
                        cursor.execute(sql_comp, vals_comp)
                    except Error as e_comp:
                         logger.error(f"     ❌ [DEBUG] Error insertando comparable #{i+1}: {e_comp}")
            else:
                # INSERCIÓN DE FILA PLACEHOLDER (No hay comparables)
                logger.info(f"     ⚪ Insertando fila placeholder en 'comparables' para {uid}")
                try:
                    vals_placeholder = (
                        uid,        # propiedad_id (OBLIGATORIO)
                        "S/I",      # fuente (S/I evita errores si la columna es muy corta o ENUM)
                        "S/I",      # rol_comparable
                        "Sin Datos",# direccion
                        "Sin Datos",# comuna
                        None,       # precio_uf
                        None,       # uf_m2
                        None,       # fecha_transaccion
                        None,       # fecha_publicacion
                        None,       # anio_construccion
                        None,       # m2_util
                        None,       # m2_total
                        None,       # dormitorios
                        None,       # banios
                        None,       # estacionamientos
                        None,       # bodegas
                        None,       # distancia_metros
                        "N/A",      # link_mapa
                        "N/A"       # link_publicacion
                    )
                    cursor.execute(sql_comp, vals_placeholder)
                except Error as e_comp:
                     # AQUÍ ESTABA EL TRUCO: Te mostrará el error real de MySQL si vuelve a fallar
                     logger.error(f"     ❌ [DEBUG] MySQL rechazó el placeholder: {e_comp}")
            
            if callback_progreso:
                callback_progreso(idx + 1, total)

        conn.commit()
        logger.success(f"✅ Inyección completada: {total} propiedades guardadas en BD.")
        return True

    # --- NUEVA SEMÁNTICA: Control explícito de Errores MySQL ---
    except Error as e:
        logger.error(f"❌ Error en transacción BD: {e}")
        if conn: conn.rollback()
        
        # Mapeo de códigos de error de MySQL (errno) a mensajes amigables
        err_code = e.errno if hasattr(e, 'errno') else 0
        if err_code in (1046, 1049, 2003, 2005):
            mensaje_bd = "No se pudo contactar al servidor o base de datos MySQL (Revise las variables de entorno de conexión)."
        elif err_code in (1406, 1264, 1366, 1292):
            mensaje_bd = "Error de formato de datos: Una de las propiedades extraídas superó el límite de caracteres o no coincide con el tipo de dato de la base de datos."
        elif err_code == 1062:
            mensaje_bd = "Error de duplicidad: El registro o rol que intenta insertar ya existe en la base de datos."
        elif err_code in (2006, 2013):
            mensaje_bd = "Se perdió la conexión con la base de datos de forma inesperada mientras se inyectaban los datos."
        else:
            mensaje_bd = f"Fallo en la base de datos durante el guardado. La inyección fue cancelada. Detalle técnico: {e.msg if hasattr(e, 'msg') else str(e)}"
            
        raise Exception(mensaje_bd)
        
    except Exception as ex:
        logger.error(f"❌ Error general en Paso 4: {ex}")
        if conn: conn.rollback()
        raise Exception(f"Error estructural procesando los datos para la inyección. Detalle técnico: {str(ex)}")

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("🔌 Conexión a BD cerrada.")