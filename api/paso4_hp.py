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
    """Convierte '18/05/2021' a '2021-05-18'. Retorna None si está vacío."""
    if not fecha_str or str(fecha_str).strip() == "":
        return None
    try:
        fecha_obj = datetime.strptime(str(fecha_str).strip(), "%d/%m/%Y")
        return fecha_obj.strftime("%Y-%m-%d")
    except ValueError:
        return None

def limpiar_precio_uf(valor: Any) -> float:
    """Transforma 'UF 4.800' -> 4800.0 (Float para decimales)."""
    if not valor: return 0.0
    s = str(valor).upper().replace("UF", "").replace(".", "").replace(",", ".").strip()
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

# --- NUEVO HELPER PARA MONTOS GRANDES ---
def limpiar_monto_entero(valor: Any) -> int:
    """
    Transforma montos monetarios a ENTEROS para BIGINT.
    'UF 2.742' -> 2742
    '$ 150.000.000' -> 150000000
    'UF 5.250,50' -> 5250 (Trunca decimales para BIGINT)
    """
    if not valor: return 0
    
    # 1. Quitamos basura de texto conocida
    s = str(valor).upper().replace("UF", "").replace("$", "").replace(".", "").strip()
    
    # 2. Si viene con coma decimal (ej: UF 5,5), cortamos la parte decimal porque BIGINT no la soporta
    if "," in s:
        s = s.split(",")[0]

    try:
        # 3. Dejamos solo dígitos
        s = re.sub(r'[^\d]', '', s)
        return int(s)
    except:
        return 0

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
    if not conn: return False

    cursor = conn.cursor()
    total = len(lista_datos)
    
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
            
            vend_str = ", ".join(trans.get("vendedores", [])) if trans.get("vendedores") else None
            comp_str = ", ".join(trans.get("compradores", [])) if trans.get("compradores") else None

            # --- 2. QUERY MAESTRA (PROPIEDADES) ---
            
            # Limpiezas críticas
            fecha_tx_clean = convertir_fecha_mysql(trans.get("fecha"))
            monto_tx_clean = limpiar_monto_entero(trans.get("monto")) # <--- APLICADO AQUÍ

            sql_propiedad = """
                INSERT INTO propiedades (
                    id, rol_sii, comuna, direccion, propietario,
                    tipo_propiedad, destino_sii, m2_construido, m2_terreno,
                    avaluo_total, avaluo_exento, avaluo_afecto, contribuciones_semestrales,
                    cbr_foja, cbr_numero, cbr_anio, fecha_transaccion, monto_transaccion,
                    vendedores, compradores, nombre_archivo_origen
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            valores_propiedad = (
                uid, gral.get("rol"), gral.get("comuna"), gral.get("direccion"), gral.get("propietario"),
                carac.get("Tipo"), carac.get("Destino"), 
                limpiar_decimal_chile(carac.get("M2 Construcción")), 
                limpiar_decimal_chile(carac.get("M2 Terreno")),
                avaluo.get("Avalúo Total"), avaluo.get("Avalúo Exento"), 
                avaluo.get("Avalúo Afecto"), avaluo.get("Contribuciones Semestrales"),
                cbr.get("Foja"), cbr.get("Número"), cbr.get("Año"), 
                fecha_tx_clean,
                monto_tx_clean, # <--- Valor limpio (int)
                vend_str, comp_str, meta.get("nombre")
            )
            
            cursor.execute(sql_propiedad, valores_propiedad)

            # --- 3. CONSTRUCCIONES ---
            for c in item.get("construcciones", []):
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

            # --- 4. ROLES ASOCIADOS ---
            mapa_deudas = {str(d.get("rol", "")).upper().strip(): d for d in item.get("deudas", [])}
            for r in item.get("roles_cbr", []):
                key = str(r.get("rol", "")).upper().strip()
                deuda_obj = mapa_deudas.get(key, {})
                
                sql_roles = """
                    INSERT INTO roles_asociados (propiedad_id, rol_asociado, tipo_ubicacion, monto_deuda, link_deuda)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql_roles, (uid, r.get("rol"), r.get("tipo"), deuda_obj.get("monto", 0), deuda_obj.get("link_tgr")))

            # --- 5. DEUDAS (SOLO ROL PRINCIPAL) ---
            rol_princ_key = str(gral.get("rol", "")).upper().strip()
            for d in item.get("deudas", []):
                if rol_princ_key in str(d.get("rol", "")).upper().strip():
                    sql_deuda = """
                        INSERT INTO deudas_tgr (propiedad_id, rol_deuda, monto, link_tgr)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(sql_deuda, (uid, d.get("rol"), d.get("monto"), d.get("link_tgr")))

            # --- 6. COMPARABLES ---
# --- 6. COMPARABLES (DEBUGGING EXTENDIDO) ---
            hp_data = item.get("house_pricing", {})
            raw_comps = hp_data.get("comparables") # Extraemos la variable cruda

            # LOGS CHIVATOS: Para ver qué demonios está llegando
            if raw_comps is None:
                logger.warning(f"   ⚠️ [DEBUG] No existe la llave 'comparables' para la propiedad {uid}")
            elif isinstance(raw_comps, str):
                logger.warning(f"   ⚠️ [DEBUG] 'comparables' es un texto (posible error de scraping): {raw_comps}")
            elif isinstance(raw_comps, list):
                if len(raw_comps) == 0:
                    logger.warning(f"   ⚠️ [DEBUG] La lista de comparables está vacía [] para {uid}")
                else:
                    logger.info(f"   👀 [DEBUG] Se encontraron {len(raw_comps)} comparables. Intentando insertar...")
                    
                    for i, comp in enumerate(raw_comps):
                        try:
                            sql_comp = """
                                INSERT INTO comparables (
                                    propiedad_id, fuente, rol_comparable, direccion, comuna,
                                    precio_uf, uf_m2, fecha_transaccion, anio_construccion,
                                    m2_util, m2_total, dormitorios, banios, distancia_metros, link_mapa, link_publicacion
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            vals_comp = (
                                uid, 
                                comp.get("fuente", "Desconocido"), 
                                comp.get("rol", ""), 
                                comp.get("direccion", ""), 
                                comp.get("comuna", ""),
                                limpiar_precio_uf(comp.get("precio_uf")),
                                limpiar_decimal_chile(comp.get("uf_m2")),
                                convertir_fecha_mysql(comp.get("fecha_transaccion")),
                                comp.get("anio", 0),
                                limpiar_decimal_chile(comp.get("m2_util")), 
                                limpiar_decimal_chile(comp.get("m2_total")), 
                                limpiar_int(comp.get("dormitorios")), 
                                limpiar_int(comp.get("banios")),
                                comp.get("distancia_metros", 0),
                                comp.get("link_maps", ""), 
                                comp.get("link_publicacion", "")
                            )
                            cursor.execute(sql_comp, vals_comp)
                        except Error as e_comp:
                             logger.error(f"   ❌ [DEBUG] Error insertando comparable #{i+1}: {e_comp}")
            else:
                 logger.error(f"   ❌ [DEBUG] Tipo de dato inesperado en 'comparables': {type(raw_comps)}")
            
            if callback_progreso:
                callback_progreso(idx + 1, total)

        conn.commit()
        logger.success(f"✅ Inyección completada: {total} propiedades guardadas en BD.")
        return True

    except Error as e:
        logger.error(f"❌ Error en transacción BD: {e}")
        if conn: conn.rollback()
        return False
        
    except Exception as ex:
        logger.error(f"❌ Error general en Paso 4: {ex}")
        if conn: conn.rollback()
        return False

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("🔌 Conexión a BD cerrada.")