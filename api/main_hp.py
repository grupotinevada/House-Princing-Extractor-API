import os
import json
import shutil
import uuid
import threading
from datetime import datetime

# --- IMPORTACIÓN DE MÓDULOS DE LÓGICA ---
from . import paso0_hp, paso1_html_hp, paso1_pdf_hp, paso2_hp, paso3_hp, paso4_hp
from logger import get_logger

# --- CONFIGURACIÓN DE RUTAS ---
CARPETA_PDFS = "./input_pdfs"
OUTPUT_FOLDER = "house_pricing_outputs"

# Archivos temporales para el flujo
TEMP_JSON_PASO1 = "temp_paso1.json"
TEMP_EXCEL = "temp_reporte.xlsx"

# Control de limpieza
ENABLE_CLEANUP = True 

logger = get_logger("main_hp", log_dir="logs", log_file="main_hp.log")

def cleanup_temp_files(cancel_event):
    """Limpia los rastros temporales del proceso actual."""
    if not ENABLE_CLEANUP:
        logger.info("ℹ️ Limpieza de archivos desactivada (modo debug).")
        return

    archivos_a_eliminar = [TEMP_JSON_PASO1, TEMP_EXCEL]
    logger.info("🧹 Ejecutando limpieza de archivos temporales...")

    for archivo in archivos_a_eliminar:
        if cancel_event.is_set(): break
        try:
            if os.path.exists(archivo):
                os.remove(archivo)
                logger.debug(f"   🗑️ Eliminado: {archivo}")
        except Exception as e:
            logger.warning(f"   ⚠️ No se pudo eliminar {archivo}: {e}")

def main(cancel_event, ruta_lista="propiedades.csv", progress_callback=None):
    """
    Orquestador Maestro del Pipeline de Tasación.
    Gestiona el flujo desde la descarga inicial hasta la inyección en BD.
    """
    try:
        logger.info("=== INICIO DEL FLUJO DE TASACIÓN MAESTRO ===")
        if progress_callback: progress_callback(0, "Iniciando Pipeline y validando directorios...")

        # 1. Asegurar estructura de directorios
        for folder in [CARPETA_PDFS, OUTPUT_FOLDER]:
            if not os.path.exists(folder):
                os.makedirs(folder)

        # ==============================================================================
        # PASO 0: DESCARGA Y CAPTURA DE SESIÓN MAESTRA (0% - 20%)
        # ==============================================================================
        logger.info(f">>> EJECUTANDO PASO 0: Descarga automática desde {ruta_lista}...")
        if progress_callback: progress_callback(2, "Paso 0: Autenticando y preparando descargas...")
        
        def cb_paso0(curr, total):
            prog = 2 + int((curr / total) * 18) # Llega hasta 20%
            if progress_callback: progress_callback(prog, f"Paso 0: Descargando propiedades ({curr}/{total})...")

        total_exitos_descarga, fallidos_paso0, cookies_maestras = paso0_hp.ejecutar(
            ruta_lista, cancel_event, callback_progreso=cb_paso0
        )

        if total_exitos_descarga == 0:
            if not cookies_maestras:
                raise Exception("❌ [FALLO PASO 0] Error de Autenticación o Red: Cloudflare/Turnstile bloqueó la conexión o las credenciales son inválidas.")
            else:
                raise Exception("❌ [FALLO PASO 0] Archivo vacío o roles no encontrados: No se pudo descargar ninguna propiedad válida de la lista ingresada.")

        logger.success(f"✅ Paso 0 completado. {total_exitos_descarga} archivos listos en disco.")
        if cancel_event.is_set(): return

        # ==============================================================================
        # PASO 1: EXTRACCIÓN DUAL RESILIENTE HTML/PDF (20% - 40%)
        # ==============================================================================
        logger.info(">>> EJECUTANDO PASO 1: Extracción técnica de datos...")
        if progress_callback: progress_callback(20, "Paso 1: Iniciando extracción de datos (HTML/PDF)...")

        def cb_paso1(curr, total):
            prog = 20 + int((curr / total) * 20) # Llega hasta 40%
            if progress_callback: progress_callback(prog, f"Paso 1: Extrayendo información de informes ({curr}/{total})...")

        # Intento A: Vía rápida por HTML
        json_propiedades = paso1_html_hp.procesar_lote_htmls(CARPETA_PDFS, cancel_event, callback_progreso=cb_paso1)    
        
        # Fallback B: Si no hay HTMLs, intenta con los PDFs de respaldo
        if not json_propiedades:
            logger.warning("⚠️ No se detectó data en HTML. Activando Fallback a lectura de PDFs...")
            json_propiedades = paso1_pdf_hp.procesar_lote_pdfs(CARPETA_PDFS, cancel_event, callback_progreso=cb_paso1)

        if not json_propiedades:
            raise Exception("❌ [FALLO PASO 1] Error de Lectura: El motor no pudo extraer datos de los archivos HTML ni de los PDF. Verifica el formato de House Pricing.")

        # --- ADUANA DE DATOS ---
        json_propiedades_validadas = []
        for prop in json_propiedades:
            rol_log = prop.get("informacion_general", {}).get("rol", "S/R")
            if prop.get("FATAL_ERROR_DATA"):
                logger.warning(f"⛔ Aduana bloqueó el rol {rol_log}: {prop.get('motivo_error')}")
                continue
            json_propiedades_validadas.append(prop)

        if not json_propiedades_validadas:
            raise Exception("❌ [FALLO PASO 1] Validación Técnica: Se extrajeron archivos, pero ninguna propiedad contenía datos válidos o roles identificables.")

        logger.success(f"✅ Paso 1 completado. {len(json_propiedades_validadas)} propiedades superaron la validación.")
        if cancel_event.is_set(): return

        # ==============================================================================
        # PASO 2: MERCADO E INYECCIÓN DE SESIÓN (40% - 80%)
        # ==============================================================================
        logger.info(">>> EJECUTANDO PASO 2: Búsqueda de comparables de mercado...")
        if progress_callback: progress_callback(40, "Paso 2: Conectando con el mercado para buscar comparables...")

        def cb_paso2(curr, total):
            prog = 40 + int((curr / total) * 40) # Llega hasta 80%
            if progress_callback: progress_callback(prog, f"Paso 2: Evaluando mercado y comparables ({curr}/{total})...")

        json_enriquecido = paso2_hp.procesar_lista_propiedades(
            json_propiedades_validadas, 
            cancel_event, 
            callback_progreso=cb_paso2,
            cookies_sesion=cookies_maestras 
        )
        
        if not json_enriquecido:
            raise Exception("❌ [FALLO PASO 2] Error de Mercado: El proceso de scraping en House Pricing falló de manera crítica y no devolvió resultados.")

        logger.success(f"✅ Paso 2 completado. Mercado evaluado para {len(json_enriquecido)} propiedades.")
        if cancel_event.is_set(): return

        # ==============================================================================
        # PASO 3: REPORTE EXCEL RELACIONAL (80% - 90%)
        # ==============================================================================
        logger.info(">>> EJECUTANDO PASO 3: Generación de estructura Excel...")
        if progress_callback: progress_callback(80, "Paso 3: Construyendo hojas de cálculo relacionales...")

        def cb_paso3(curr, total):
            prog = 80 + int((curr / total) * 10) # Llega hasta 90%
            if progress_callback: progress_callback(prog, f"Paso 3: Escribiendo registros en Excel ({curr}/{total})...")

        nombre_final_excel = f"Reporte_Tasacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        exito_excel = paso3_hp.generar_excel(
            json_enriquecido, 
            cancel_event, 
            nombre_archivo=TEMP_EXCEL, 
            callback_progreso=cb_paso3, 
            crear_excel=False   
        )
        
        if not exito_excel:
            raise Exception("❌ [FALLO PASO 3] Error de Archivo: No se pudo escribir y guardar el archivo Excel temporal en disco.")
        
        logger.success("✅ Paso 3 completado. Archivo Excel temporal generado.")
        if cancel_event.is_set(): return

        # ==============================================================================
        # PASO 4: PERSISTENCIA MySQL (90% - 100%)
        # ==============================================================================
        logger.info(">>> EJECUTANDO PASO 4: Inyección de datos en MySQL...")
        if progress_callback: progress_callback(90, "Paso 4: Sincronizando información con la base de datos...")
        
        def cb_paso4(curr, total):
            prog = 90 + int((curr / total) * 10) # Llega hasta 100%
            if progress_callback: progress_callback(prog, f"Paso 4: Guardando registros en BD ({curr}/{total})...")

        try:
            exito_bd = paso4_hp.insertar_datos(
                lista_datos=json_enriquecido, 
                cancel_event=cancel_event, 
                callback_progreso=cb_paso4
            )
            
            if exito_bd:
                logger.success("✅ Paso 4 completado: Datos persistidos exitosamente en MySQL.")
            else:
                logger.warning("⚠️ [ADVERTENCIA PASO 4]: El proceso de guardado en BD retornó 'False' o fue cancelado por el usuario.")
                
        except Exception as e_db:
            logger.error(f"⚠️ [ERROR PASO 4]: Fallo en la inserción a BD ({str(e_db)}). El proceso continuará para garantizar la entrega del archivo Excel.")

        # ==============================================================================
        # CIERRE Y LIMPIEZA
        # ==============================================================================
        ruta_final = os.path.join(OUTPUT_FOLDER, nombre_final_excel)
        
        if os.path.exists(TEMP_EXCEL):
            shutil.move(TEMP_EXCEL, ruta_final)
            logger.info(f"📁 Reporte disponible en: {ruta_final}")
        else:
            logger.warning(f"⚠️ No se encontró el archivo temporal de Excel ({TEMP_EXCEL}) para mover.")
        
        cleanup_temp_files(cancel_event)
        
        logger.info(f"🎉 === PIPELINE FINALIZADO CON ÉXITO ===")
        if progress_callback: progress_callback(100, "¡Proceso Completado Exitosamente!")
        
        return True

    except Exception as e:
        logger.error(f"❌ INTERRUPCIÓN CRÍTICA DEL PIPELINE: {str(e)}")
        # Forzamos limpieza de temporales para no dejar basura si el programa se cae
        cleanup_temp_files(cancel_event)
        raise e

if __name__ == "__main__":
    # Prueba local de ejecución
    evento_cancelacion = threading.Event()
    try:
        main(evento_cancelacion, ruta_lista="propiedades.csv")
    except Exception as error_main:
        print(f"Ejecución detenida: {error_main}")