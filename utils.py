import psutil
import os
from logger import get_logger

logger = get_logger("utils_monitor", log_dir="logs", log_file="utils.log")

def obtener_uso_recursos():
    """
    Calcula el consumo total de CPU y RAM del proceso actual (API/Worker)
    Y de todos sus hijos (especialmente Chrome/Chromedriver).
    Retorna un diccionario.
    """
    try:
        # 1. Proceso Padre (Python actual)
        proc_actual = psutil.Process(os.getpid())
        
        # Inicializamos contadores
        total_ram_bytes = proc_actual.memory_info().rss
        total_cpu_percent = proc_actual.cpu_percent(interval=None)
        num_procesos_chrome = 0
        
        # 2. Buscamos recursivamente a todos los hijos (Workers, Drivers, Browsers)
        hijos = proc_actual.children(recursive=True)
        
        for hijo in hijos:
            try:
                # Sumar RAM
                total_ram_bytes += hijo.memory_info().rss
                # Sumar CPU (puede ser > 100% en multicore)
                total_cpu_percent += hijo.cpu_percent(interval=None)
                
                # Contar instancias de Chrome para detectar "Zombies" o carga
                if "chrome" in hijo.name().lower() or "google" in hijo.name().lower():
                    num_procesos_chrome += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # El proceso murió mientras lo leíamos, lo ignoramos
                pass

        # 3. Conversión a MB
        total_ram_mb = round(total_ram_bytes / (1024 * 1024), 2)
        
        # 4. Info Global del Sistema (Para saber si estamos al límite)
        memoria_virtual = psutil.virtual_memory()
        
        return {
            "ram_uso_mb": total_ram_mb,
            "ram_sistema_percent": memoria_virtual.percent,
            "cpu_proceso_percent": round(total_cpu_percent, 1),
            "cpu_sistema_percent": psutil.cpu_percent(interval=None),
            "workers_chrome_activos": num_procesos_chrome,
            "num_hilos_totales": len(hijos) + 1
        }

    except Exception as e:
        logger.error(f"Error midiendo recursos: {e}")
        return {
            "error": str(e)
        }

def matar_procesos_zombies():
    """
    Mata forzosamente todos los procesos hijos (Chrome/Chromedriver)
    del proceso actual. Útil para la limpieza en cancelaciones.
    """
    try:
        proc_actual = psutil.Process(os.getpid())
        hijos = proc_actual.children(recursive=True)
        
        count = 0
        for hijo in hijos:
            try:
                hijo.kill()
                count += 1
            except:
                pass
        
        if count > 0:
            logger.warning(f"🧹 Se eliminaron {count} procesos huérfanos/zombies.")
    except Exception as e:
        logger.error(f"Error matando zombies: {e}")