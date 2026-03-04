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

# ======================================================
# Catálogo Traductor de Comunas para Selenium
# clave = comuna normalizada
# valor = texto exacto mostrado en el sitio
# ======================================================
# Catálogo Traductor: { "comuna_aplastada": "Nombre Exacto Para Selenium" }
# Catálogo Traductor: { "comuna_aplastada": "Nombre Exacto Para Selenium" }
COMUNAS_TRADUCTOR = {
    "arica": "Arica",
    "camarones": "Camarones",
    "putre": "Putre",
    "general lagos": "General Lagos",
    "iquique": "Iquique",
    "alto hospicio": "Alto Hospicio",
    "pozo almonte": "Pozo Almonte",
    "camina": "Camiña",
    "colchane": "Colchane",
    "huara": "Huara",
    "pica": "Pica",
    "antofagasta": "Antofagasta",
    "mejillones": "Mejillones",
    "sierra gorda": "Sierra Gorda",
    "taltal": "Taltal",
    "calama": "Calama",
    "ollague": "Ollagüe",
    "san pedro de atacama": "San Pedro de Atacama",
    "tocopilla": "Tocopilla",
    "maria elena": "María Elena",
    "copiapo": "Copiapó",
    "caldera": "Caldera",
    "tierra amarilla": "Tierra Amarilla",
    "chanaral": "Chañaral",
    "diego de almagro": "Diego de Almagro",
    "vallenar": "Vallenar",
    "freirina": "Freirina",
    "huasco": "Huasco",
    "alto del carmen": "Alto del Carmen",
    "la serena": "La Serena",
    "coquimbo": "Coquimbo",
    "andacollo": "Andacollo",
    "la higuera": "La Higuera",
    "paihuano": "Paihuano",
    "vicuna": "Vicuña",
    "ovalle": "Ovalle",
    "monte patria": "Monte Patria",
    "combarbala": "Combarbalá",
    "punitaqui": "Punitaqui",
    "rio hurtado": "Río Hurtado",
    "illapel": "Illapel",
    "salamanca": "Salamanca",
    "los vilos": "Los Vilos",
    "canela": "Canela",
    "valparaiso": "Valparaíso",
    "vina del mar": "Viña del Mar",
    "quintero": "Quintero",
    "puchuncavi": "Puchuncaví",
    "quilpue": "Quilpué",
    "villa alemana": "Villa Alemana",
    "casablanca": "Casablanca",
    "concon": "Concón",
    "juan fernandez": "Juan Fernández",
    "la ligua": "La Ligua",
    "petorca": "Petorca",
    "cabildo": "Cabildo",
    "zapallar": "Zapallar",
    "papudo": "Papudo",
    "los andes": "Los Andes",
    "san esteban": "San Esteban",
    "calle larga": "Calle Larga",
    "rinconada": "Rinconada",
    "san felipe": "San Felipe",
    "putaendo": "Putaendo",
    "santa maria": "Santa María",
    "panquehue": "Panquehue",
    "llaillay": "Llaillay",
    "catemu": "Catemu",
    "quillota": "Quillota",
    "la cruz": "La Cruz",
    "calera": "Calera",
    "nogales": "Nogales",
    "hijuelas": "Hijuelas",
    "limache": "Limache",
    "olmue": "Olmué",
    "san antonio": "San Antonio",
    "cartagena": "Cartagena",
    "el quisco": "El Quisco",
    "el tabo": "El Tabo",
    "algarrobo": "Algarrobo",
    "santo domingo": "Santo Domingo",
    "isla de pascua": "Isla de Pascua",
    "santiago": "Santiago",
    "cerrillos": "Cerrillos",
    "cerro navia": "Cerro Navia",
    "conchali": "Conchalí",
    "el bosque": "El Bosque",
    "estacion central": "Estación Central",
    "independencia": "Independencia",
    "la cisterna": "La Cisterna",
    "la florida": "La Florida",
    "la granja": "La Granja",
    "la reina": "La Reina",
    "las condes": "Las Condes",
    "lo barnechea": "Lo Barnechea",
    "lo espejo": "Lo Espejo",
    "lo prado": "Lo Prado",
    "macul": "Macul",
    "maipu": "Maipú",
    "nunoa": "Ñuñoa",
    "pedro aguirre cerda": "Pedro Aguirre Cerda",
    "penalolen": "Peñalolén",
    "providencia": "Providencia",
    "pudahuel": "Pudahuel",
    "puente alto": "Puente Alto",
    "quilicura": "Quilicura",
    "quinta normal": "Quinta Normal",
    "recoleta": "Recoleta",
    "renca": "Renca",
    "san joaquin": "San Joaquín",
    "san miguel": "San Miguel",
    "san ramon": "San Ramón",
    "vitacura": "Vitacura",
    "buin": "Buin",
    "calera de tango": "Calera de Tango",
    "san bernardo": "San Bernardo",
    "paine": "Paine",
    "talagante": "Talagante",
    "el monte": "El Monte",
    "isla de maipo": "Isla de Maipo",
    "padre hurtado": "Padre Hurtado",
    "colina": "Colina",
    "lampa": "Lampa",
    "tiltil": "Tiltil",
    "alhue": "Alhué",
    "curacavi": "Curacaví",
    "maria pinto": "María Pinto",
    "melipilla": "Melipilla",
    "rancagua": "Rancagua",
    "graneros": "Graneros",
    "mostazal": "Mostazal",
    "codegua": "Codegua",
    "machali": "Machalí",
    "olivar": "Olivar",
    "requinoa": "Requínoa",
    "rengo": "Rengo",
    "malloa": "Malloa",
    "quinta de tilcoco": "Quinta de Tilcoco",
    "san vicente": "San Vicente",
    "pichidegua": "Pichidegua",
    "peumo": "Peumo",
    "coltauco": "Coltauco",
    "coinco": "Coinco",
    "donihue": "Doñihue",
    "las cabras": "Las Cabras",
    "san fernando": "San Fernando",
    "chimbarongo": "Chimbarongo",
    "placilla": "Placilla",
    "nancagua": "Nancagua",
    "chepica": "Chépica",
    "santa cruz": "Santa Cruz",
    "lolol": "Lolol",
    "pumanque": "Pumanque",
    "palmilla": "Palmilla",
    "peralillo": "Peralillo",
    "pichilemu": "Pichilemu",
    "navidad": "Navidad",
    "litueche": "Litueche",
    "la estrella": "La Estrella",
    "marchihue": "Marchihue",
    "paredones": "Paredones",
    "talca": "Talca",
    "constitucion": "Constitución",
    "empedrado": "Empedrado",
    "maule": "Maule",
    "pelarco": "Pelarco",
    "pencahue": "Pencahue",
    "rio claro": "Río Claro",
    "san clemente": "San Clemente",
    "san rafael": "San Rafael",
    "curepto": "Curepto",
    "licanten": "Licantén",
    "rauco": "Rauco",
    "sagrada familia": "Sagrada Familia",
    "teno": "Teno",
    "vichuquen": "Vichuquén",
    "curico": "Curicó",
    "colbun": "Colbún",
    "linares": "Linares",
    "longavi": "Longaví",
    "parral": "Parral",
    "retiro": "Retiro",
    "san javier": "San Javier",
    "villa alegre": "Villa Alegre",
    "yerbas buenas": "Yerbas Buenas",
    "chillan": "Chillán",
    "chillan viejo": "Chillán Viejo",
    "bulnes": "Bulnes",
    "cobquecura": "Cobquecura",
    "coelemu": "Coelemu",
    "coihueco": "Coihueco",
    "el carmen": "El Carmen",
    "ninhue": "Ninhue",
    "niquen": "Ñiquén",
    "pemuco": "Pemuco",
    "pinto": "Pinto",
    "portezuelo": "Portezuelo",
    "quillon": "Quillón",
    "quirihue": "Quirihue",
    "ranquil": "Ránquil",
    "san carlos": "San Carlos",
    "san fabian": "San Fabián",
    "san ignacio": "San Ignacio",
    "san nicolas": "San Nicolás",
    "treguaco": "Treguaco",
    "yungay": "Yungay",
    "los angeles": "Los Ángeles",
    "antuco": "Antuco",
    "cabrero": "Cabrero",
    "laja": "Laja",
    "mulchen": "Mulchén",
    "nacimiento": "Nacimiento",
    "negrete": "Negrete",
    "quilaco": "Quilaco",
    "quilleco": "Quilleco",
    "san rosendo": "San Rosendo",
    "santa barbara": "Santa Bárbara",
    "tucapel": "Tucapel",
    "temuco": "Temuco",
    "padre las casas": "Padre Las Casas",
    "carahue": "Carahue",
    "cunco": "Cunco",
    "curarrehue": "Curarrehue",
    "freire": "Freire",
    "galvarino": "Galvarino",
    "gorbea": "Gorbea",
    "lautaro": "Lautaro",
    "loncoche": "Loncoche",
    "melipeuco": "Melipeuco",
    "nueva imperial": "Nueva Imperial",
    "pitrufquen": "Pitrufquén",
    "pucon": "Pucón",
    "saavedra": "Saavedra",
    "teodoro schmidt": "Teodoro Schmidt",
    "tolten": "Toltén",
    "villarrica": "Villarrica",
    "valdivia": "Valdivia",
    "corral": "Corral",
    "lanco": "Lanco",
    "los lagos": "Los Lagos",
    "mafil": "Máfil",
    "mariquina": "Mariquina",
    "paillaco": "Paillaco",
    "panguipulli": "Panguipulli",
    "futrono": "Futrono",
    "la union": "La Unión",
    "lago ranco": "Lago Ranco",
    "rio bueno": "Río Bueno",
    "osorno": "Osorno",
    "puyehue": "Puyehue",
    "puerto octay": "Puerto Octay",
    "purranque": "Purranque",
    "rio negro": "Río Negro",
    "san juan de la costa": "San Juan de la Costa",
    "san pablo": "San Pablo",
    "castro": "Castro",
    "ancud": "Ancud",
    "chonchi": "Chonchi",
    "curaco de velez": "Curaco de Vélez",
    "dalcahue": "Dalcahue",
    "puqueldon": "Puqueldón",
    "queilen": "Queilén",
    "quinchao": "Quinchao",
    "quellon": "Quellón",
    "calbuco": "Calbuco",
    "fresia": "Fresia",
    "frutillar": "Frutillar",
    "los muermos": "Los Muermos",
    "llanquihue": "Llanquihue",
    "maullin": "Maullín",
    "puerto montt": "Puerto Montt",
    "puerto varas": "Puerto Varas",
    "cochamo": "Cochamó",
    "hualaihue": "Hualaihué",
    "chaiten": "Chaitén",
    "futaleufu": "Futaleufú",
    "palena": "Palena",
    "aysen": "Aysén",
    "cisnes": "Cisnes",
    "guaitecas": "Guaitecas",
    "coihaique": "Coyhaique",
    "lago verde": "Lago Verde",
    "chile chico": "Chile Chico",
    "rio ibanez": "Río Ibáñez",
    "cochrane": "Cochrane",
    "o'higgins": "O'Higgins",
    "tortel": "Tortel",
    "punta arenas": "Punta Arenas",
    "laguna blanca": "Laguna Blanca",
    "rio verde": "Río Verde",
    "san gregorio": "San Gregorio",
    "porvenir": "Porvenir",
    "primavera": "Primavera",
    "timaukel": "Timaukel",
    "natales": "Natales",
    "torres del paine": "Torres del Paine",
    "cabo de hornos": "Cabo de Hornos",
    "antartica": "Antártica"
}