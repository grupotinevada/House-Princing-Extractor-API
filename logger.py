# logger.py
import logging
import os
import sys
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)  # Inicializa colorama

DEBUG_CONSOLE = True    # Desarrollo

# --- 1. DEFINICIÓN DEL NIVEL PERSONALIZADO 'SUCCESS' ---
# Python no tiene SUCCESS por defecto (INFO=20, WARNING=30). 
# Lo definimos en 25 (entre INFO y WARNING).
SUCCESS_LEVEL_NUM = 25
logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")

def success(self, message, *args, **kws):
    if self.isEnabledFor(SUCCESS_LEVEL_NUM):
        self._log(SUCCESS_LEVEL_NUM, message, args, **kws)

# "Inyectamos" el método success a la clase Logger para poder usar logger.success()
logging.Logger.success = success
logging.SUCCESS = SUCCESS_LEVEL_NUM

# -------------------------------------------------------

class ColoredFormatter(logging.Formatter):
    """Formatter que agrega colores según nivel de log en consola"""
    LEVEL_COLORS = {
        logging.DEBUG: Fore.WHITE,       # Blanco (dim)
        logging.INFO: Fore.CYAN,         # Cian
        logging.SUCCESS: Fore.GREEN,     # Verde (NUEVO)
        logging.WARNING: Fore.YELLOW,    # Amarillo
        logging.ERROR: Fore.RED,         # Rojo
        logging.CRITICAL: Fore.MAGENTA   # Magenta
    }

    def format(self, record):
        # Si el nivel existe en nuestro dict, usamos su color, si no, blanco
        color = self.LEVEL_COLORS.get(record.levelno, Fore.WHITE)
        
        # Formateamos el mensaje base
        msg = super().format(record)
        
        # Devolvemos string coloreado + reset
        return f"{color}{msg}{Style.RESET_ALL}"

def get_logger(name: str, log_dir: str = "logs", log_file: str = None,
               level_console=None, level_file=logging.DEBUG) -> logging.Logger:

    """
    Devuelve un logger configurado para archivo y consola con colores en consola.
    """
    os.makedirs(log_dir, exist_ok=True)

    if log_file is None:
        log_file = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capturamos todo a nivel raíz

    # Definir nivel de consola por defecto
    if level_console is None:
        level_console = logging.DEBUG if DEBUG_CONSOLE else logging.INFO

    # Evitar duplicar handlers si get_logger se llama varias veces
    if logger.handlers:
        # Opcional: Actualizar niveles si ya existe
        return logger

    # --- FORMATOS ---
    # En archivo: Fecha completa y nivel claro
    fmt_file = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter_file = logging.Formatter(fmt_file)

    # En consola: Más limpio, con colores
    fmt_console = "%(asctime)s | %(name)s | [%(levelname)s]     | %(message)s"
    # Ajustamos el formato de fecha para consola (solo hora)
    formatter_console = ColoredFormatter(fmt_console, datefmt="%H:%M:%S")

    # --- HANDLER ARCHIVO ---
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level_file)
    fh.setFormatter(formatter_file)
    logger.addHandler(fh)

    # --- HANDLER CONSOLA ---
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level_console)
    ch.setFormatter(formatter_console)
    logger.addHandler(ch)

    return logger

def log_section(logger: logging.Logger, name: str):
    """Marca una sección destacada en los logs"""
    logger.info("")
    logger.info("="*10 + f" [ {name} ] " + "="*10)

def dbg(logger: logging.Logger, msg: str):
    """Función rápida para debug"""
    logger.debug(msg)