import os
import json
import shutil
import uuid
from datetime import datetime

# Importamos los módulos de lógica
from . import paso0_hp, paso1_hp, paso2_hp, paso3_hp, paso4_hp
from logger import get_logger

# --- CONFIGURACIÓN ---
CARPETA_PDFS = "./input_pdfs"
OUTPUT_FOLDER = "house_pricing_outputs"

# Archivos temporales
TEMP_JSON_PASO1 = "temp_paso1.json"
TEMP_JSON_FINAL = "temp_final.json"
TEMP_EXCEL = "temp_reporte.xlsx"

ENABLE_CLEANUP = True 

logger = get_logger("main_hp", log_dir="logs", log_file="main_hp.log")

# CORRECCIÓN: Agregar cancel_event
def cleanup_temp_files(cancel_event):
    if not ENABLE_CLEANUP:
        logger.info("ℹ️ Limpieza de archivos desactivada (modo debug).")
        return

    archivos_a_eliminar = [TEMP_JSON_PASO1]

    logger.info("🧹 Ejecutando limpieza de archivos temporales...")

    # Eliminar archivos temporales
    for archivo in archivos_a_eliminar:
        if cancel_event.is_set():
            logger.info("🛑 Proceso cancelado por usuario.")
            return
        try:
            if os.path.exists(archivo):
                os.remove(archivo)
                logger.info(f"   -> Eliminado: {archivo}")
        except Exception as e:
            logger.warning(f"   ⚠️ No se pudo eliminar {archivo}: {e}")

    # Eliminar carpeta completa y recrearla (Windows y Linux)
    try:
        if os.path.exists(CARPETA_PDFS):
            shutil.rmtree(CARPETA_PDFS)
            logger.info(f"   -> Carpeta eliminada: {CARPETA_PDFS}")

        os.makedirs(CARPETA_PDFS, exist_ok=True)
        logger.info(f"   -> Carpeta recreada: {CARPETA_PDFS}")

    except Exception as e:
        logger.warning(f"   ⚠️ No se pudo resetear {CARPETA_PDFS}: {e}")


def calcular_progreso_global(paso_idx, items_procesados, total_items, callback):
    """
    Calcula el % global basado en 5 pasos con pesos ponderados por duración estimada:
    - Paso 0 (Descargas): 0-15%
    - Paso 1 (PDF): 15-30%
    - Paso 2 (Selenium): 30-80% (El más lento)
    - Paso 3 (Excel): 80-90%
    - Paso 4 (Base de Datos): 90-100%
    """
    if callback:
        # Definición de rangos
        bases = [0, 15, 30, 80, 90]  
        pesos = [15, 15, 50, 10, 10] 

        # Protección por si el índice se sale
        idx = paso_idx if 0 <= paso_idx < 5 else 0
        
        base = bases[idx]
        rango = pesos[idx]

        if total_items > 0:
            avance_relativo = (items_procesados / total_items) * rango
        else:
            avance_relativo = rango # Si no hay items, asumimos paso completado
        
        total_global = base + avance_relativo
        
        # Tope visual estético
        if total_global > 99: total_global = 99
        
        nombres_pasos = ["Descargando", "Extrayendo PDF", "Buscando Precios", "Generando Excel", "Guardando en BD"]
        mensaje = f"{nombres_pasos[idx]} ({items_procesados}/{total_items})"
        
        callback(round(total_global, 1), mensaje)


# CORRECCIÓN: Agregar cancel_event
def main(cancel_event, ruta_lista=None, progress_callback=None):
    logger.info("=== INICIO DEL FLUJO DE TASACIÓN ===")

    # ------------------------------------------------------------------
    # PASO 0: Descarga Automática (0% - 25%)
    # ------------------------------------------------------------------
    if ruta_lista:
        if cancel_event.is_set(): return
        
        logger.info(f">>> EJECUTANDO PASO 0: Descarga automática desde {ruta_lista}...")
        if progress_callback: progress_callback(5, "Iniciando descargas...")
        # Callback local para Paso 0
        def cb_paso0(proc, tot):
            calcular_progreso_global(0, proc, tot, progress_callback)

        # Ejecuta la lógica de descarga con callback
        exito_paso0 = paso0_hp.ejecutar(ruta_lista, cancel_event, callback_progreso=cb_paso0)
        
        if not exito_paso0:
            if cancel_event.is_set():
                logger.warning("Proceso cancelado en Paso 0.")
            else:
                mensaje_error = "❌ El Paso 0 falló o no se descargaron archivos. Abortando."
                logger.error(mensaje_error)
            raise Exception(mensaje_error)

        logger.info("✅ Paso 0 completado. PDFs listos en carpeta de entrada.")

    # ------------------------------------------------------------------
    # PASO 1: Procesar PDFs (25% - 50%)
    # ------------------------------------------------------------------
    if cancel_event.is_set(): return

    logger.info(">>> EJECUTANDO PASO 1: Extracción masiva de PDFs...")
    
    def cb_paso1(proc, tot):
        calcular_progreso_global(1, proc, tot, progress_callback)

    # Se pasa cancel_event y callback
    json_propiedades = paso1_hp.procesar_lote_pdfs(CARPETA_PDFS, cancel_event, callback_progreso=cb_paso1)    

    if not json_propiedades:
        if cancel_event.is_set():
            logger.warning("Proceso cancelado en Paso 1.")
        else:
            logger.error("❌ No se generó ningún JSON válido en el Paso 1. Abortando.")
        return

    logger.info(f"✅ Paso 1 completado. {len(json_propiedades)} propiedades extraídas.")
    
    with open(TEMP_JSON_PASO1, "w", encoding="utf-8") as f:
        json.dump(json_propiedades, f, indent=4, ensure_ascii=False)

    # ------------------------------------------------------------------
    # PASO 2: Búsqueda Selenium (50% - 75%)
    # ------------------------------------------------------------------
    if cancel_event.is_set(): return

    logger.info(">>> EJECUTANDO PASO 2: Búsqueda de mercado (Selenium)...")
    
    def cb_paso2(proc, tot):
        calcular_progreso_global(2, proc, tot, progress_callback)

    # Pasar cancel_event y callback
    json_enriquecido = paso2_hp.procesar_lista_propiedades(json_propiedades, cancel_event, callback_progreso=cb_paso2)

    if cancel_event.is_set() or not json_enriquecido:
        logger.warning("Paso 2 cancelado o sin resultados.")
        return

    logger.info(f"✅ Paso 2 completado. Datos enriquecidos con comparables.")

    with open(TEMP_JSON_FINAL, "w", encoding="utf-8") as f:
        json.dump(json_enriquecido, f, indent=4, ensure_ascii=False)

    # ------------------------------------------------------------------
    # PASO 3: Generar Excel (75% - 100%)
    # ------------------------------------------------------------------
    if cancel_event.is_set(): return

    logger.info(">>> EJECUTANDO PASO 3: Generación de Excel...")
    
    def cb_paso3(proc, tot):
        calcular_progreso_global(3, proc, tot, progress_callback)

    # Pasar cancel_event y callback
    exito_excel = paso3_hp.generar_excel(json_enriquecido, cancel_event, TEMP_EXCEL, callback_progreso=cb_paso3)

    # ------------------------------------------------------------------
    # PASO 4: Inyección a Base de Datos (90% - 100%)
    # ------------------------------------------------------------------
    if cancel_event.is_set(): return

    logger.info(">>> EJECUTANDO PASO 4: Inyección a Base de Datos...")
    
    def cb_paso4(proc, tot):
        calcular_progreso_global(4, proc, tot, progress_callback)

    # Inyectamos usando el JSON enriquecido del Paso 2
    exito_bd = paso4_hp.insertar_datos(json_enriquecido, cancel_event, callback_progreso=cb_paso4)

    if not exito_bd:
        logger.error("⚠️ Hubo errores guardando en la Base de Datos (ver logs), pero el Excel se generó correctamente.")
        # No hacemos return aquí para permitir que el usuario descargue el Excel aunque falle la BD.

    logger.info("✅ Paso 4 completado.")
    
    if exito_excel:
        logger.info(">>> FINALIZANDO: Moviendo archivos y limpieza...")
        
        if not os.path.exists(OUTPUT_FOLDER):
            os.makedirs(OUTPUT_FOLDER)

        fecha_str = datetime.now().strftime("%Y-%m-%d")
        uuid_str = uuid.uuid4().hex[:6]
        base_name = f"Reporte_HousePricing_{fecha_str}_{uuid_str}"

        ruta_final_json = os.path.join(OUTPUT_FOLDER, f"{base_name}.json")
        ruta_final_excel = os.path.join(OUTPUT_FOLDER, f"{base_name}.xlsx")

        try:
            if os.path.exists(TEMP_EXCEL):
                shutil.move(TEMP_EXCEL, ruta_final_excel)
                logger.info(f"📂 Excel guardado en: {ruta_final_excel}")
            
            if os.path.exists(TEMP_JSON_FINAL):
                shutil.move(TEMP_JSON_FINAL, ruta_final_json)
                logger.info(f"📂 JSON Raw guardado en: {ruta_final_json}")

            cleanup_temp_files(cancel_event)

            logger.info("🎉 === PROCESO FINALIZADO CON ÉXITO === 🎉")
            if progress_callback: progress_callback(100, "Proceso Completado Exitosamente")
            
            # Bloque seguro para Linux/API (No intentar abrir GUI)
            try:
                if os.name == 'nt': # Solo en Windows
                    os.startfile(ruta_final_excel)
            except:
                pass

        except Exception as e:
            logger.error(f"❌ Error moviendo archivos finales: {e}")

    else:
        if cancel_event.is_set():
            logger.warning("Proceso cancelado en Paso 3.")
        else:
            logger.error("❌ El Paso 3 falló generando el Excel. Revisa los logs.")

if __name__ == "__main__":
    if not os.path.exists(CARPETA_PDFS):
        os.makedirs(CARPETA_PDFS)
    else:
        import threading
        main(threading.Event())