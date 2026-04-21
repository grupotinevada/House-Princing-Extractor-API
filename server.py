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
from pydantic import BaseModel, field_validator, Field
import time
import utils
import re
import unicodedata


# --- 1. CONFIGURACIÓN DE RUTAS E IMPORTS ---
# Definimos la Raíz del Proyecto (Donde está este archivo server.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv()
SEMAFORO_PROCESAMIENTO = threading.Lock()

class PropiedadRequest(BaseModel):
    rol: str
    comuna: str
    direccion: Optional[str] = None

    @field_validator('rol')
    @classmethod
    def limpiar_y_validar_rol(cls, v: str) -> str:
        # 1. Limpieza inicial: Quitar palabra "rol", espacios y normalizar guiones
        # Reemplaza guion largo (—) y en-dash (–) por guion normal (-)
        v_limpio = v.lower().replace("rol", "").replace(" ", "").strip()
        v_limpio = v_limpio.replace("—", "-").replace("–", "-")
        
        # 2. Manejo de ceros a la izquierda y formato
        if '-' in v_limpio:
            partes = v_limpio.split('-')
            if len(partes) == 2:
                manzana = partes[0].lstrip('0')
                predio = partes[1].lstrip('0')
                
                # Si al quitar ceros queda vacío (era "000"), dejamos un "0"
                manzana = manzana if manzana else "0"
                predio = predio if predio else "0"
                
                v_limpio = f"{manzana}-{predio.upper()}"

        # 3. Validación final: Debe ser números, un guion, y números o la letra K
        if not re.match(r'^\d+-[\dK]+$', v_limpio):
            raise ValueError(f"Formato de Rol inválido ('{v}'). Se intentó reparar como '{v_limpio}'. Debe ser 'Manzana-Predio'.")
        
        return v_limpio

    @field_validator('comuna')
    @classmethod
    def limpiar_y_validar_comuna(cls, v: str) -> str:
        from utils import COMUNAS_TRADUCTOR
        # 1. La "jugera": convierte "ÑUÑOA", "ñuñoa", "Nunoa" -> "nunoa"
        v_norm = unicodedata.normalize('NFKD', v).encode('ASCII', 'ignore').decode('utf-8').lower().strip()
        
        # 2. Buscamos si "nunoa" existe en el diccionario de utils.py
        if v_norm not in utils.COMUNAS_TRADUCTOR:
            raise ValueError(f"La comuna '{v}' no es reconocida en nuestro sistema. Verifique la ortografía.")
        
        # 3. Retornamos la versión perfecta desde utils para que Selenium nunca falle.
        return utils.COMUNAS_TRADUCTOR[v_norm]

def get_db_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
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
    allow_origins=["*"], # Permite conexiones desde cualquier origen (tu PHP)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
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
def ejecutar_proceso_background(task_id: str, file_path: str, cancel_event: threading.Event):
    """
    Esta función corre en un hilo separado gestionado por FastAPI.
    Llama a main_hp.main() y actualiza el diccionario global 'tasks'.
    """
    logger.info(f"🚀 [API] Iniciando tarea {task_id} con archivo: {file_path}")
    
    # Creamos el evento de cancelación para este hilo
    # cancel_event = threading.Event()
    stop_monitor_event = threading.Event()

    # Actualizamos el estado inicial
    if task_id in tasks:
        tasks[task_id]["status"] = "queued"
        tasks[task_id]["message"] = "⏳ Servidor ocupado. Tu tarea está en cola de espera..."
        tasks[task_id]["progress"] = 0
        tasks[task_id]["stats"] = {}
        tasks[task_id]["errores_parciales"] = []

    logger.info(f"🚦 [API] Tarea {task_id} esperando su turno...")

    def monitor_loop():
        """Hilo que mide RAM/CPU cada 2 segundos"""
        while not stop_monitor_event.is_set():
            if task_id in tasks:
                # Llamamos a utils.py
                datos = utils.obtener_uso_recursos()
                tasks[task_id]["stats"] = datos
                logger.info(
                    f"📊 [MONITOR {task_id[:4]}] "
                    f"RAM: {datos.get('ram_uso_mb')}MB ({datos.get('ram_sistema_percent')}%) | "
                    f"CPU: {datos.get('cpu_proceso_percent')}% | "
                    f"Chrome Zombies: {datos.get('workers_chrome_activos')}"
                )
            time.sleep(15)

    # Definimos el callback que main_hp llamará para reportar progreso
    def progress_callback_api(porcentaje, mensaje, errores_nuevos=None):
        if task_id in tasks:
            # Si recibimos señal de cancelación, forzamos estado
            if cancel_event.is_set():
                tasks[task_id]["status"] = "cancelled"
                return

            tasks[task_id]["progress"] = round(porcentaje, 1)
            tasks[task_id]["message"] = mensaje

            if errores_nuevos:
                if "errores_parciales" not in tasks[task_id]:
                    tasks[task_id]["errores_parciales"] = []
                tasks[task_id]["errores_parciales"].extend(errores_nuevos)

            # Log de hitos importantes para no saturar la consola del server
            if porcentaje == 0 or porcentaje == 25 or porcentaje == 50 or porcentaje == 75 or porcentaje == 100:
                logger.info(f"📊 Task {task_id}: {porcentaje}% - {mensaje}")

    def partial_error_callback_api(rol: str, paso: str, motivo: str):
        if task_id in tasks:
            # Aseguramos que la lista exista (aunque FastAPI la inicializa por defecto)
            if "errores_parciales" not in tasks[task_id]:
                tasks[task_id]["errores_parciales"] = []
            
            tasks[task_id]["errores_parciales"].append({
                "rol": rol,
                "paso": paso,
                "motivo": motivo
            })
            logger.warning(f"⚠️ [API] Error parcial registrado para Rol {rol}: {motivo}")

    # === INICIO DEL SISTEMA DE COLA ===
    # El hilo se detendrá aquí si hay otro proceso ejecutándose
    with SEMAFORO_PROCESAMIENTO:
        if cancel_event.is_set():
            logger.warning(f"🛑 Tarea {task_id} cancelada antes de salir de la cola.")
            tasks[task_id]["status"] = "cancelled"
            if os.path.exists(file_path): os.remove(file_path)
            return
        logger.info(f"🟢 [API] Turno concedido a {task_id}. Iniciando...")

        hilo_monitor = threading.Thread(target=monitor_loop, daemon=True)
        hilo_monitor.start()

        # Actualizamos estado a "Procesando" ahora que tenemos el lock
        if task_id in tasks:
            tasks[task_id]["status"] = "processing"
            tasks[task_id]["message"] = "🚀 Iniciando procesamiento..."

        try:
            # --- LLAMADA MAESTRA A LA LÓGICA ---
            # Llamamos al main modificado. 
            # IMPORTANTE: main_hp.py ya debe tener la firma: main(cancel_event, ruta_lista, progress_callback)
            exito = main_hp.main(
                cancel_event=cancel_event,
                ruta_lista=file_path,
                progress_callback=progress_callback_api,
                partial_error_callback=partial_error_callback_api
            )
            
            # Verificación Post-Ejecución
            if cancel_event.is_set():
                tasks[task_id]["status"] = "cancelled"
                tasks[task_id]["message"] = "Tarea cancelada por el usuario."
                utils.matar_procesos_zombies()
            
            elif exito:
                # --- MODIFICACIÓN: EXITO GARANTIZADO ---
                # Si main_hp devolvió True, marcamos como completado INMEDIATAMENTE.
                tasks[task_id]["status"] = "completed"
                tasks[task_id]["progress"] = 100
                tasks[task_id]["message"] = "Proceso finalizado exitosamente."
                
                # Intentamos buscar el archivo solo para habilitar el botón de descarga
                try:
                    list_of_files = glob.glob(os.path.join(OUTPUT_DIR, '*.xlsx'))
                    if list_of_files:
                        latest_file = max(list_of_files, key=os.path.getctime)
                        tasks[task_id]["result_file"] = latest_file
                        logger.info(f"✅ [API] Archivo vinculado: {os.path.basename(latest_file)}")
                    else:
                        logger.warning(f"⚠️ Proceso OK. No se vinculó Excel (probablemente desactivado).")
                except Exception as e:
                    logger.warning(f"⚠️ Error menor buscando archivo de salida: {e}")
            
            else:
                tasks[task_id]["status"] = "error"
                tasks[task_id]["message"] = "El proceso reportó un fallo interno (ver logs de lógica)."

        except Exception as e:
            logger.error(f"❌ [API] Excepción no controlada en Task {task_id}: {e}", exc_info=True)
            tasks[task_id]["status"] = "error"
            # MODIFICACIÓN: Mostrar el mensaje limpio de la excepción
            tasks[task_id]["message"] = str(e)
            utils.matar_procesos_zombies()

        finally:
            stop_monitor_event.set()
            hilo_monitor.join(timeout=1)
            if cancel_event.is_set():
                tasks[task_id]["status"] = "cancelled"
                tasks[task_id]["message"] = "Proceso detenido completamente."
            # Limpieza del archivo subido (Input)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except: 
                    pass            
    # === FIN DEL SISTEMA DE COLA (Se libera el semáforo automáticamente) ===


# --- 4. ENDPOINTS ---
@app.on_event("startup")
def startup_event():
    logger.info("Iniciando validación de sistema (BD y Variables)...")
    # 1. Validar variables de entorno vitales
    required_envs = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "LOGIN_URL", "URL_ANTECEDENTES", "URL_TASACIONES", "USUARIO_HP", "PASSWORD_HP"]
    missing = [v for v in required_envs if not os.getenv(v)]
    if missing:
        msg = f"❌ ERROR CRÍTICO: Faltan variables en .env: {', '.join(missing)}"
        logger.error(msg)
        print(msg)
        os._exit(1)
        
    # 2. Validar conexión a la Base de Datos
    conn = get_db_connection()
    if not conn or not conn.is_connected():
        msg = "❌ ERROR CRÍTICO: No se pudo conectar a la BD al iniciar el servidor."
        logger.error(msg)
        print(msg)
        os._exit(1)
    conn.close()
    logger.info("✅ Validaciones de inicio exitosas. API lista.")
    
@app.get(
    "/health",
    tags=["Sistema"],
    summary="Verificar estado de la API",
    description="Endpoint ligero para comprobar si la API está viva y respondiendo correctamente.",
    response_description="Retorna el estado de conexión actual y el sistema operativo."
)
def health_check():
    """Endpoint para verificar que la API está viva"""
    return {"status": "online", "system": sys.platform}

@app.post(
    "/upload-process",
    tags=["Procesamiento"],
    summary="Subir archivo y procesar",
    description="""
    Recibe un archivo Excel (.xlsx, .xls) o CSV (.csv), lo guarda temporalmente en el servidor, 
    genera un ID de tarea único (`task_id`) y lanza el procesamiento de propiedades en un hilo en segundo plano (background task).
    """,
    response_description="Retorna el ID único de la tarea generada y el estado inicial de la cola."
)
async def upload_and_process(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    """
    Recibe el archivo Excel/CSV, genera un ID y lanza el proceso en background.
    """

    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        logger.warning(f"Intento de subida bloqueado: {file.filename}")
        raise HTTPException(status_code=400, detail="Formato de archivo no soportado. Use solo .csv, .xlsx o .xls")
    
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
    cancel_event = threading.Event()
    # Inicializar estado en memoria
    tasks[task_id] = {
        "status": "queued",
        "progress": 0,
        "message": "En cola...",
        "filename": file.filename,
        "cancel_event": cancel_event # Se crea dentro del thread
    }

    # Encolar tarea
    background_tasks.add_task(ejecutar_proceso_background, task_id, file_location, cancel_event)

    return {
        "task_id": task_id, 
        "status": "queued", 
        "message": "Archivo recibido. Proceso iniciado."
    }

@app.post(
    "/process-json",
    tags=["Procesamiento"],
    summary="Procesar propiedades vía JSON",
    description="""
    Recibe un listado de propiedades directamente en formato JSON, validando la estructura mediante Pydantic. 
    Convierte internamente los datos a un archivo temporal y lanza el procesamiento en segundo plano.
    """,
    response_description="Retorna el ID único de la tarea y confirma el inicio del proceso."
)
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
    cancel_event = threading.Event()
    # 3. Inicializar estado en memoria
    tasks[task_id] = {
        "status": "queued",
        "progress": 0,
        "message": "Datos JSON recibidos. En cola...",
        "filename": "Entrada_JSON.json",
        "cancel_event": cancel_event
    }

    # 4. Encolar la misma función de background que usa el upload
    background_tasks.add_task(ejecutar_proceso_background, task_id, file_location, cancel_event)

    return {
        "task_id": task_id, 
        "status": "queued", 
        "message": "Datos recibidos correctamente. Proceso iniciado."
    }
from typing import Dict, List, Any
from fastapi import HTTPException, Path
from pydantic import BaseModel, Field

# Simulación del storage global
tasks: Dict[str, Dict[str, Any]] = {}

class ErrorDetalle(BaseModel):
    rol: str
    paso: str
    motivo: str

class StatusResponse(BaseModel):
    task_id: str = Field(..., description="Identificador único universal (UUID) de la tarea.")
    status: str = Field(
        ..., 
        description="Estado actual: 'queued', 'processing', 'completed', 'error', 'cancelling' o 'cancelled'."
    )
    progress: float = Field(
        ..., 
        ge=0, 
        le=100, 
        description="Porcentaje de avance del proceso (0.0 a 100.0)."
    )
    message: str = Field(
        ..., 
        description="Mensaje informativo sobre la etapa actual del procesamiento."
    )
    system_stats: Dict[str, Any] = Field(
        default_factory=dict,
        description="Métricas de hardware: uso de RAM, CPU y cantidad de navegadores activos."
    )
    errores_parciales: List[ErrorDetalle] = Field(
        default_factory=list,
        description="Lista de advertencias o fallos no críticos encontrados durante la ejecución."
    )


@app.get(
    "/status/{task_id}",
    tags=["Monitoreo"],
    summary="Consultar estado y métricas de la tarea",
    response_model=StatusResponse,
    responses={
        200: {"description": "Estado obtenido correctamente."},
        404: {"description": "La tarea especificada no existe en la memoria volátil del servidor."}
    }
)
def get_status(
    task_id: str = Path(
        ..., 
        description="El UUID retornado al iniciar el proceso", 
        example="550e8400-e29b-41d4-a716-446655440000"
    )
):
    task = tasks.get(task_id)

    if task is None:
        raise HTTPException(status_code=404, detail="Tarea no encontrada o ID inválido")

    return StatusResponse(
        task_id=task_id,
        status=task.get("status", "unknown"),
        progress=task.get("progress", 0.0),
        message=task.get("message", ""),
        system_stats=task.get("stats", {}),
        errores_parciales=task.get("errores_parciales", [])
    )

@app.get(
    "/download/{task_id}",
    tags=["Descargas"],
    summary="Descargar Excel de resultados",
    description="Si el proceso asociado al `task_id` ha finalizado exitosamente (`status == 'completed'`), permite descargar el archivo físico con los resultados extraídos.",
    response_class=FileResponse,
    response_description="Descarga de archivo tipo application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
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

@app.post(
    "/cancel/{task_id}",
    tags=["Procesamiento"],
    summary="Cancelar una tarea en curso",
    description="Activa el evento de detención seguro (`threading.Event`) que cierra navegadores Zombies, finaliza el script asíncrono y libera recursos antes de que termine su ciclo natural.",
    response_description="Confirmación de la ejecución de la señal de cancelación."
)
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
            task["status"] = "cancelling" # No mentimos, está en proceso de apagar.
            task["message"] = "Deteniendo procesos de forma segura... (Puede tardar unos segundos)"
            logger.info(f"🛑 Solicitud de cancelación recibida para {task_id}")
            return {"message": "Señal de cancelación enviada. Apagando navegadores..."}
        else:
             return {"message": "El proceso no se pudo cancelar (aún no inicializado)."}
    
    return {"message": f"El proceso no se puede cancelar (Estado actual: {status})."}


@app.get(
    "/api/datos/{nombre_tabla}",
    tags=["Datos e Información"],
    summary="Consultar últimos registros de DB",
    description="Extrae y retorna los últimos 100 registros directamente desde la base de datos MySQL. Se aplica una lista blanca de tablas para evitar inyecciones SQL.",
    response_description="Lista de diccionarios representando los últimos 100 registros extraídos de la tabla seleccionada."
)
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