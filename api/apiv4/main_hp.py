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
        
        callback(round(total_global, 1), mensaje, None)


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
        
        def cb_paso0(proc, tot):
            calcular_progreso_global(0, proc, tot, progress_callback)

        # CAMBIO: Ahora desempaquetamos la tupla (cantidad_exitos, lista_fallidos)
        cant_exitos, lista_fallidos = paso0_hp.ejecutar(ruta_lista, cancel_event, callback_progreso=cb_paso0)
        
        # Lógica de Validación de Errores para la API
        if cant_exitos == 0:
            mensaje_error = "❌ Fallo general en descargas."
            
            # Analizamos por qué fallaron
            if lista_fallidos:
                total_fallos = len(lista_fallidos)
                # Contamos cuántos fueron por 'Rol no encontrado'
                roles_inexistentes = [f for f in lista_fallidos if f.get('motivo_error') == "Rol no encontrado"]
                cant_inexistentes = len(roles_inexistentes)

                if cant_inexistentes == total_fallos:
                    # CASO 1: Todos fallaron porque el rol no existe (Error de Datos del Usuario)
                    mensaje_error = f"❌ Error: Ninguno de los {total_fallos} roles ingresados existe en House Pricing. Verifique comuna y rol."
                elif cant_inexistentes > 0:
                    # CASO 2: Mezcla
                    mensaje_error = f"❌ Fallo total: {cant_inexistentes} roles no existen y el resto falló por conexión."
                else:
                    # CASO 3: Error técnico (Timeout, Login, etc)
                    motivo = lista_fallidos[0].get('motivo_error', 'Desconocido')
                    mensaje_error = f"❌ Error técnico en descargas: {motivo}."
            else:
                # --- NUEVO: CASO 4: Archivo vacío o extensión inválida (.txt) ---
                mensaje_error = "❌ Error técnico: Archivo no soportado o sin datos válidos."

            if cancel_event.is_set():
                logger.warning("Proceso cancelado en Paso 0.")
                return
            else:
                logger.error(mensaje_error)
                # AL LANZAR ESTA EXCEPCIÓN, server.py la captura y la pone en el JSON 'message'
                raise Exception(mensaje_error)
        
        if lista_fallidos and cant_exitos > 0:
            errores_p0 = [{"rol": f.get('rol', 'Desconocido'), "paso": "Descarga PDF", "motivo": f.get('motivo_error', 'Fallo en descarga')} for f in lista_fallidos]
            if progress_callback:
                progress_callback(25, f"Descargas listas. {len(lista_fallidos)} fallaron.", errores_p0)
            logger.warning(f"⚠️ Paso 0 completado con {len(lista_fallidos)} fallos.")
        else:
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
            return
        else:
            mensaje_error = "❌ No se generó ningún JSON válido en el Paso 1. Abortando."
            logger.error(mensaje_error)
            raise Exception(mensaje_error)

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

    if cancel_event.is_set():
        logger.warning("Paso 2 cancelado por usuario.")
        return
    
    if not json_enriquecido:
        mensaje_error = "❌ El Paso 2 terminó sin resultados válidos."
        logger.error(mensaje_error)
        raise Exception(mensaje_error)
    
    json_limpio = []
    errores_p2 = []

    for prop in json_enriquecido:
        rol_actual = prop.get("informacion_general", {}).get("rol", "S/R")
        tasa_uf = prop.get("tasa_vta_uf", 0)

        if not tasa_uf or str(tasa_uf).strip() in ["0", "0.0", ""]:
            motivo = "Propiedad sin Tasación capturada (Fallo en origen o valor 0)."
            logger.warning(f"⛔ Aduana bloqueó el rol {rol_actual}: {motivo}")
            errores_p2.append({"rol": rol_actual, "paso": "Validación Tasación", "motivo": motivo})
            continue # Se descarta, NO llegará a la Base de Datos
        
        if prop.get("FATAL_ERROR_DATA"):
            motivo = prop.get("motivo_error", "Error crítico en extracción de datos esenciales.")
            logger.warning(f"⛔ Aduana bloqueó el rol {rol_actual}: {motivo}")
            errores_p2.append({"rol": rol_actual, "paso": "Extracción HP", "motivo": motivo})
            continue

        estado_hp = prop.get("house_pricing", {}).get("comparables", [])
        if isinstance(estado_hp, str) and ("Error" in estado_hp or "Sin resultados" in estado_hp):
            errores_p2.append({"rol": rol_actual, "paso": "Búsqueda HP", "motivo": estado_hp})
        
        json_limpio.append(prop)
        
    if not json_limpio:
        mensaje_error = "❌ Ninguna propiedad del lote superó la Aduana de Datos"
        logger.error(mensaje_error)
        raise Exception(mensaje_error)
    
    if errores_p2 and progress_callback:
        progress_callback(75, f"Scraping listo. {len(json_limpio)} válidos, {len(errores_p2)} bloqueados/sin resultados.", errores_p2)
        logger.warning(f"⚠️ Paso 2 completado con {len(errores_p2)} bloqueos o advertencias.")
    else:
        logger.info(f"✅ Paso 2 completado. {len(json_limpio)} propiedades perfectas listas para DB/Excel.")

    with open(TEMP_JSON_FINAL, "w", encoding="utf-8") as f:
        # ATENCIÓN: Guardamos solo la lista limpia
        json.dump(json_limpio, f, indent=4, ensure_ascii=False)

    json_enriquecido = json_limpio
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

    # --- CORRECCIÓN PUNTO 4: VALIDACIÓN ESTRICTA Y ERROR AL FRONT ---
    if not exito_bd:
        mensaje_error = "❌ Error crítico: Falló la inyección en la BD (Rollback ejecutado)."
        logger.error(mensaje_error)
        raise Exception(mensaje_error)

    logger.info("✅ Paso 4 completado.")
    
# Reemplaza desde "if exito_excel:" hasta el final del archivo por esto:

    if exito_excel:
        # --- CORRECCIÓN PUNTO 3: PREVENIR FALSA CANCELACIÓN EXITOSA ---
        if cancel_event.is_set():
            logger.warning("Proceso cancelado justo antes de finalizar.")
            return False

        logger.info(">>> FINALIZANDO: Moviendo archivos y limpieza...")
        
        if not os.path.exists(OUTPUT_FOLDER):
            os.makedirs(OUTPUT_FOLDER)

        fecha_str = datetime.now().strftime("%Y-%m-%d")
        uuid_str = uuid.uuid4().hex[:6]
        base_name = f"Reporte_HousePricing_{fecha_str}_{uuid_str}"

        ruta_final_json = os.path.join(OUTPUT_FOLDER, f"{base_name}.json")
        ruta_final_excel = os.path.join(OUTPUT_FOLDER, f"{base_name}.xlsx")

        try:
            if exito_excel == "SKIPPED":
                logger.info("⏩ Se omitió la creación del Excel por configuración. Solo se guardará el JSON.")
            elif exito_excel is True and os.path.exists(TEMP_EXCEL):
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
                if os.name == 'nt' and exito_excel is True: # Solo intentar abrir si realmente se creó
                    os.startfile(ruta_final_excel)
            except:
                pass
            
            # --- CORRECCIÓN: Retornamos True explícitamente al servidor ---
            return True

        except Exception as e:
            # --- CORRECCIÓN: Si falla al mover el archivo (ej: Excel lo tiene bloqueado), la API debe saberlo ---
            mensaje_error = f"❌ Error guardando los archivos finales: {e}"
            logger.error(mensaje_error)
            raise Exception(mensaje_error)

    else:
        if cancel_event.is_set():
            logger.warning("Proceso cancelado en Paso 3.")
            return
        else:
            # --- CORRECCIÓN: La excepción del Paso 3 ahora está en el lugar correcto ---
            mensaje_error = "❌ El Paso 3 falló generando el Excel."
            logger.error(mensaje_error)
            raise Exception(mensaje_error)

# --- CORRECCIÓN: Bloque Main restaurado a la normalidad ---
if __name__ == "__main__":
    if not os.path.exists(CARPETA_PDFS):
        os.makedirs(CARPETA_PDFS)
    
    import threading
    # Solo para pruebas locales de main_hp.py
    main(threading.Event())