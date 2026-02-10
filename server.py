############################################################################################################################
#  SERVER.PY - CEREBRO DE LA API
#  Encargado de recibir peticiones HTTP, gestionar la cola de tareas y lanzar el proceso main_hp en background.
#  NO contiene lógica de negocio, solo orquestación web.
############################################################################################################################

import shutil
import os
import uuid
import sys
import threading
import glob
from typing import Dict, Optional,List
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import mysql.connector
from dotenv import load_dotenv
from pydantic import BaseModel

# --- 1. CONFIGURACIÓN DE RUTAS E IMPORTS ---
# Definimos la Raíz del Proyecto (Donde está este archivo server.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv()


class PropiedadRequest(BaseModel):
    rol: str
    comuna: str
    direccion: Optional[str] = None

def get_db_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "house_pricing_db")
        )
    except Exception as e:
        print(f"Error DB: {e}")
        return None
# Definimos dónde están los scripts de lógica (Asumiendo que están en la carpeta 'api')
API_DIR = os.path.join(BASE_DIR, "api")

# --- PARCHE DE IMPORTS PARA LINUX/DOCKER ---
# Esto permite que main_hp.py pueda hacer 'import paso0_hp' aunque lo llamemos desde server.py
if API_DIR not in sys.path:
    sys.path.append(API_DIR)
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Importamos la lógica de negocio
# NOTA: Asegúrate de que exista un archivo vacío __init__.py dentro de la carpeta 'api'
try:
    from api import main_hp
except ImportError:
    # Fallback por si los archivos están en la raíz junto a server.py
    import main_hp

from logger import get_logger

# Configuración del Logger del Servidor
logger = get_logger("server_api", log_dir=os.path.join(BASE_DIR, "logs"), log_file="server.log")

# Inicializamos la App FastAPI
app = FastAPI(title="House Pricing API Worker")

# Configuración CORS (Permite que tu PHP/HTML local se conecte sin bloqueos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. GESTIÓN DE ESTADO (MEMORIA) ---
# Estructura: task_id -> { status, progress, message, result_file, cancel_event }
tasks: Dict[str, Dict] = {}

# Definición de Carpetas de Trabajo
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
# Esta carpeta debe coincidir con la que usa main_hp.py
OUTPUT_DIR = os.path.join(BASE_DIR, "house_pricing_outputs") 

# Crear carpetas si no existen
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- 3. WORKER DE FONDO (BACKGROUND TASK) ---
def ejecutar_proceso_background(task_id: str, file_path: str):
    """
    Esta función corre en un hilo separado gestionado por FastAPI.
    Llama a main_hp.main() y actualiza el diccionario global 'tasks'.
    """
    logger.info(f"🚀 [API] Iniciando tarea {task_id} con archivo: {file_path}")
    
    # Creamos el evento de cancelación para este hilo
    cancel_event = threading.Event()
    
    # Actualizamos el estado inicial
    if task_id in tasks:
        tasks[task_id]["cancel_event"] = cancel_event
        tasks[task_id]["status"] = "processing"
        tasks[task_id]["progress"] = 0

    # Definimos el callback que main_hp llamará para reportar progreso
    def progress_callback_api(porcentaje, mensaje):
        if task_id in tasks:
            # Si recibimos señal de cancelación, forzamos estado
            if cancel_event.is_set():
                tasks[task_id]["status"] = "cancelled"
                return

            tasks[task_id]["progress"] = round(porcentaje, 1)
            tasks[task_id]["message"] = mensaje
            
            # Log de hitos importantes para no saturar la consola del server
            if porcentaje == 0 or porcentaje == 25 or porcentaje == 50 or porcentaje == 75 or porcentaje == 100:
                logger.info(f"📊 Task {task_id}: {porcentaje}% - {mensaje}")

    try:
        # --- LLAMADA MAESTRA A LA LÓGICA ---
        # Llamamos al main modificado. 
        # IMPORTANTE: main_hp.py ya debe tener la firma: main(cancel_event, ruta_lista, progress_callback)
        exito = main_hp.main(
            cancel_event=cancel_event,
            ruta_lista=file_path,
            progress_callback=progress_callback_api
        )
        
        # Verificación Post-Ejecución
        if tasks[task_id]["status"] == "cancelled":
            logger.warning(f"🛑 [API] Tarea {task_id} finalizó como cancelada.")
        
        elif exito is False:
             tasks[task_id]["status"] = "error"
             tasks[task_id]["message"] = "El proceso reportó un fallo interno (ver logs de lógica)."
        
        else:
            # BUSCAR EL ARCHIVO RESULTANTE
            # main_hp guarda en house_pricing_outputs. Buscamos el xlsx más reciente.
            list_of_files = glob.glob(os.path.join(OUTPUT_DIR, '*.xlsx')) 
            
            if list_of_files:
                latest_file = max(list_of_files, key=os.path.getctime)
                tasks[task_id]["status"] = "completed"
                tasks[task_id]["progress"] = 100
                tasks[task_id]["result_file"] = latest_file
                tasks[task_id]["message"] = "Proceso finalizado con éxito."
                logger.success(f"✅ [API] Tarea {task_id} completada. Archivo: {os.path.basename(latest_file)}")
            else:
                tasks[task_id]["status"] = "error"
                tasks[task_id]["message"] = "El proceso terminó OK, pero no se encontró el Excel generado."
                logger.error(f"❌ [API] Tarea {task_id} terminó sin archivo de salida en {OUTPUT_DIR}.")

    except Exception as e:
        logger.error(f"❌ [API] Excepción no controlada en Task {task_id}: {e}", exc_info=True)
        tasks[task_id]["status"] = "error"
        tasks[task_id]["message"] = f"Error interno del servidor: {str(e)}"
    
    finally:
        # Limpieza del archivo subido (Input)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except: 
                pass


# --- 4. ENDPOINTS ---

@app.get("/health")
def health_check():
    """Endpoint para verificar que la API está viva"""
    return {"status": "online", "system": sys.platform, "root": BASE_DIR}

@app.post("/upload-process")
async def upload_and_process(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    """
    Recibe el archivo Excel/CSV, genera un ID y lanza el proceso en background.
    """
    # Generar ID único
    task_id = str(uuid.uuid4())
    
    # Guardar archivo temporalmente
    safe_filename = file.filename.replace(" ", "_")
    file_location = os.path.join(UPLOAD_DIR, f"{task_id}_{safe_filename}")
    
    try:
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        logger.error(f"Error guardando upload: {e}")
        raise HTTPException(status_code=500, detail="Error guardando el archivo.")

    # Inicializar estado en memoria
    tasks[task_id] = {
        "status": "queued",
        "progress": 0,
        "message": "En cola...",
        "filename": file.filename,
        "cancel_event": None # Se crea dentro del thread
    }

    # Encolar tarea
    background_tasks.add_task(ejecutar_proceso_background, task_id, file_location)

    return {
        "task_id": task_id, 
        "status": "queued", 
        "message": "Archivo recibido. Proceso iniciado."
    }

@app.post("/process-json")
async def process_json_data(
    data: List[PropiedadRequest], 
    background_tasks: BackgroundTasks
):
    """
    Recibe un listado de propiedades en formato JSON y lanza el proceso.
    Ejemplo: [{"rol": "123-4", "comuna": "SANTIAGO"}]
    """
    if not data:
        raise HTTPException(status_code=400, detail="La lista de propiedades no puede estar vacía.")

    # 1. Generar ID de tarea
    task_id = str(uuid.uuid4())
    
    # 2. Convertir JSON a un CSV temporal para que main_hp lo pueda leer
    # Usamos CSV con separador ';' para mantener compatibilidad con paso0_hp.py
    file_location = os.path.join(UPLOAD_DIR, f"{task_id}_data.csv")
    
    try:
        import pandas as pd
        # Convertimos la lista de modelos Pydantic a una lista de diccionarios
        df = pd.DataFrame([item.model_dump() for item in data])
        df.to_csv(file_location, index=False, sep=';', encoding='utf-8')
    except Exception as e:
        logger.error(f"Error creando archivo temporal desde JSON: {e}")
        raise HTTPException(status_code=500, detail="Error interno al procesar los datos.")

    # 3. Inicializar estado en memoria
    tasks[task_id] = {
        "status": "queued",
        "progress": 0,
        "message": "Datos JSON recibidos. En cola...",
        "filename": "Entrada_JSON.json",
        "cancel_event": None
    }

    # 4. Encolar la misma función de background que usa el upload
    background_tasks.add_task(ejecutar_proceso_background, task_id, file_location)

    return {
        "task_id": task_id, 
        "status": "queued", 
        "message": "Datos recibidos correctamente. Proceso iniciado."
    }
    
@app.get("/status/{task_id}")
def get_status(task_id: str):
    """
    Retorna el estado actual del proceso (Polling).
    """
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    
    return {
        "task_id": task_id,
        "status": task["status"],
        "progress": task["progress"],
        "message": task.get("message", "")
    }

@app.get("/download/{task_id}")
def download_result(task_id: str):
    """
    Descarga el Excel resultante si el proceso terminó.
    """
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail="El proceso no ha terminado exitosamente.")
    
    file_path = task.get("result_file")
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=500, detail="El archivo resultante no se encuentra en el servidor.")

    return FileResponse(
        path=file_path, 
        filename=os.path.basename(file_path),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.post("/cancel/{task_id}")
def cancel_process(task_id: str):
    """
    Envía señal de parada al proceso.
    """
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")
    
    status = task["status"]
    if status in ["processing", "queued"]:
        event = task.get("cancel_event")
        if event:
            event.set() # Señalizamos a Python para detenerse
            task["status"] = "cancelled"
            task["message"] = "Cancelando..."
            logger.info(f"🛑 Solicitud de cancelación recibida para {task_id}")
            return {"message": "Señal de cancelación enviada."}
        else:
             return {"message": "El proceso no se pudo cancelar (aún no inicializado)."}
    
    return {"message": f"El proceso no se puede cancelar (Estado actual: {status})."}


@app.get("/api/datos/{nombre_tabla}")
def obtener_datos_tabla(nombre_tabla: str):
    """
    Retorna los últimos 100 registros de cualquier tabla permitida.
    """
    # 1. Seguridad: Lista blanca de tablas para evitar SQL Injection
    tablas_permitidas = [
        "propiedades", 
        "construcciones", 
        "roles_asociados", 
        "deudas_tgr", 
        "comparables"
    ]
    
    if nombre_tabla not in tablas_permitidas:
        raise HTTPException(status_code=400, detail="Tabla no permitida o inexistente")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")
    
    registros = []
    try:
        cursor = conn.cursor(dictionary=True) # Importante: Devuelve diccionarios {columna: valor}
        
        # 2. Query Dinámica Segura (Usamos f-string solo porque validamos nombre_tabla antes)
        # Limitamos a 100 para no saturar el navegador
        sql = f"SELECT * FROM {nombre_tabla} ORDER BY id DESC LIMIT 100"
        
        cursor.execute(sql)
        registros = cursor.fetchall()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            
    return registros

if __name__ == "__main__":
    import uvicorn
    logger.info("🌍 Iniciando Servidor API House Pricing...")
    # Ejecuta en 0.0.0.0 para acceso externo (Docker/Red Local)
    uvicorn.run(app, host="0.0.0.0", port=8181)
