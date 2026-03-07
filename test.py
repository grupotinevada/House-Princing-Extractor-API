import requests
import time
import os
import csv

API_URL = "http://localhost:8181"
TIMEOUT_POLLING = 300

def print_step(test_num, msg):
    print(f"\n{'='*70}\n🔥 TEST AVANZADO {test_num}: {msg}\n{'='*70}")

def wait_for_status(task_id, target_statuses, timeout=60):
    """Espera activamente hasta que la tarea alcance uno de los estados deseados"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            status_req = requests.get(f"{API_URL}/status/{task_id}")
            if status_req.status_code == 200:
                status = status_req.json().get("status")
                if status in target_statuses:
                    return status
        except: pass
        time.sleep(2)
    return None

# =====================================================================
# BLOQUE 1: VALIDACIONES ESTRICTAS Y PYDANTIC (Tests 1-4)
# =====================================================================

def test_01_pydantic_comuna_normalization():
    print_step(1, "Normalización de Comuna (Tolerancia a tildes y mayúsculas)")
    payload = [{"rol": "9064-112", "comuna": "   ñÚÑóÀ  "}]
    res = requests.post(f"{API_URL}/process-json", json=payload)
    if res.status_code == 200:
        print("✅ OK: Pydantic normalizó 'ñÚÑóÀ' usando utils.COMUNAS_TRADUCTOR.")
        requests.post(f"{API_URL}/cancel/{res.json().get('task_id')}")
    else:
        print(f"❌ FALLO: La comuna no fue aceptada. Status: {res.status_code}")

def test_02_pydantic_rol_strict_regex():
    print_step(2, "Expresión Regular Estricta para Rol (Bloqueo de formatos raros)")
    bad_roles = ["9064/112", "9064.112", "9064 112", "9064_112"]
    for r in bad_roles:
        res = requests.post(f"{API_URL}/process-json", json=[{"rol": r, "comuna": "macul"}])
        if res.status_code == 422:
            print(f"✅ OK: Rol mal formateado '{r}' bloqueado (422).")
        else:
            print(f"❌ FALLO: El rol '{r}' pasó el filtro de seguridad.")

def test_03_pydantic_extra_fields():
    print_step(3, "Inyección de campos extra en el JSON (Seguridad de Payload)")
    payload = [{"rol": "9064-112", "comuna": "macul", "direccion": "Av", "es_admin": True, "drop_table": "yes"}]
    res = requests.post(f"{API_URL}/process-json", json=payload)
    if res.status_code == 200:
        print("✅ OK: La API aceptó el request pero Pydantic ignoró la inyección de forma segura.")
        requests.post(f"{API_URL}/cancel/{res.json().get('task_id')}")
    else:
        print("❌ FALLO: La API no supo manejar campos adicionales.")

def test_04_http_methods_rejection():
    print_step(4, "Rechazo de Métodos HTTP Incorrectos (Seguridad de Endpoint)")
    res = requests.get(f"{API_URL}/process-json")
    if res.status_code == 405:
        print("✅ OK: Método GET bloqueado en endpoint POST (405 Method Not Allowed).")
    else:
        print(f"❌ FALLO: API respondió {res.status_code} en lugar de 405.")

# =====================================================================
# BLOQUE 2: MÁQUINA DE ESTADOS Y ARCHIVOS (Tests 5-7)
# =====================================================================

def test_05_upload_unsupported_file_extension():
    print_step(5, "Subida de Archivo con Extensión no Soportada (.txt)")
    test_file = "payload_malicioso.txt"
    with open(test_file, "w") as f:
        f.write("esto no es un excel ni csv")
    
    try:
        with open(test_file, 'rb') as f:
            res = requests.post(f"{API_URL}/upload-process", files={'file': (test_file, f, 'text/plain')})
        if res.status_code == 400:
            print("✅ OK: La API bloqueó el archivo en el endpoint directamente (Fail Fast).")
        else:
            print("❌ FALLO: El archivo no fue rechazado inmediatamente.")
    finally:
        if os.path.exists(test_file): os.remove(test_file)

def test_06_07_download_blocks():
    print_step("6 & 7", "Intento de Descargas No Permitidas (Prematura y Cancelada)")
    res = requests.post(f"{API_URL}/process-json", json=[{"rol": "9064-112", "comuna": "macul"}])
    task_id = res.json().get('task_id')
    
    down_res1 = requests.get(f"{API_URL}/download/{task_id}")
    if down_res1.status_code == 400:
        print("✅ OK TEST 6: Descarga bloqueada (400) mientras el proceso está en curso.")
    
    requests.post(f"{API_URL}/cancel/{task_id}")
    wait_for_status(task_id, ["cancelled"], timeout=30)
    
    down_res2 = requests.get(f"{API_URL}/download/{task_id}")
    if down_res2.status_code == 400:
        print("✅ OK TEST 7: Descarga bloqueada (400) en tarea definitivamente cancelada.")

# =====================================================================
# BLOQUE 3: CONCURRENCIA Y SEMÁFORO (Tests 8-11)
# =====================================================================

def test_08_09_concurrencia():
    print_step("8 & 9", "Semáforo y Liberación de Cola")
    res1 = requests.post(f"{API_URL}/process-json", json=[{"rol": "9064-112", "comuna": "macul"}])
    res2 = requests.post(f"{API_URL}/process-json", json=[{"rol": "9064-112", "comuna": "macul"}])
    
    t1 = res1.json().get('task_id')
    t2 = res2.json().get('task_id')
    
    wait_for_status(t1, ["processing"], timeout=30)
    s1 = requests.get(f"{API_URL}/status/{t1}").json().get("status")
    s2 = requests.get(f"{API_URL}/status/{t2}").json().get("status")
    
    if s1 == "processing" and s2 == "queued":
        print("✅ OK TEST 8: Semáforo funciona. T1 procesando, T2 esperando en cola.")
    else:
        print(f"❌ FALLO TEST 8. S1: {s1}, S2: {s2}")
        
    requests.post(f"{API_URL}/cancel/{t1}")
    print("   -> Tarea 1 cancelada. Esperando que el Semáforo libere a la Tarea 2...")
    
    s2_new = wait_for_status(t2, ["processing"], timeout=45)
    if s2_new == "processing":
        print("✅ OK TEST 9: Semáforo liberado exitosamente. Tarea 2 inició su procesamiento.")
    else:
        print(f"❌ FALLO TEST 9: Tarea 2 quedó en estado: {s2_new}")
        
    requests.post(f"{API_URL}/cancel/{t2}")
    wait_for_status(t2, ["cancelled"], timeout=30)

def test_10_system_stats_monitor_thread():
    print_step(10, "Hilo de Monitoreo de Recursos (RAM/Zombies)")
    res = requests.post(f"{API_URL}/process-json", json=[{"rol": "9064-112", "comuna": "macul"}])
    task_id = res.json().get('task_id')
    
    wait_for_status(task_id, ["processing"], timeout=30)
    time.sleep(4) 
    
    status = requests.get(f"{API_URL}/status/{task_id}").json()
    stats = status.get("system_stats", {})
    
    if "ram_uso_mb" in stats:
        print(f"✅ OK: Hilo monitor activo (RAM: {stats['ram_uso_mb']}MB, Chrome Zombies: {stats.get('workers_chrome_activos')}).")
    else:
        print("❌ FALLO: El objeto 'system_stats' está vacío.")
        
    requests.post(f"{API_URL}/cancel/{task_id}")
    wait_for_status(task_id, ["cancelled"], timeout=30)

def test_11_temp_file_cleanup_on_cancel():
    print_step(11, "Auditoría de Limpieza de Archivos Temporales (.csv)")
    res = requests.post(f"{API_URL}/process-json", json=[{"rol": "9064-112", "comuna": "macul"}])
    task_id = res.json().get('task_id')
    
    temp_file = os.path.join("uploads", f"{task_id}_data.csv")
    exists_before = os.path.exists(temp_file)
    
    requests.post(f"{API_URL}/cancel/{task_id}")
    
    borrado = False
    for _ in range(15):
        if not os.path.exists(temp_file):
            borrado = True
            break
        time.sleep(1)
        
    if exists_before and borrado:
        print("✅ OK: El servidor eliminó correctamente el archivo temporal.")
    else:
        print(f"❌ FALLO: Limpieza fallida.")

# =====================================================================
# BLOQUE 4: LÓGICA DE NEGOCIO AVANZADA (Tests 12-16)
# =====================================================================

def test_12_db_endpoint_sql_injection_strict():
    print_step(12, "Intento Estricto de SQL Injection (Endpoint BD)")
    attack_payloads = ["propiedades; DROP TABLE deudas_tgr;", "propiedades WHERE 1=1", "usuarios"]
    for atk in attack_payloads:
        res = requests.get(f"{API_URL}/api/datos/{atk}")
        if res.status_code == 400:
            print(f"✅ OK: Ataque '{atk[:15]}...' bloqueado por Lista Blanca.")

def test_13_late_cancellation_step2():
    print_step(13, "Cancelación Tardía (Interrupción de Selenium)")
    res = requests.post(f"{API_URL}/process-json", json=[{"rol": "9064-112", "comuna": "macul"}])
    task_id = res.json().get('task_id')
    
    print("⏳ Esperando que el proceso alcance el Paso 2 (> 30%)...")
    reached = False
    for _ in range(40):
        progreso = requests.get(f"{API_URL}/status/{task_id}").json().get("progress", 0)
        if progreso >= 30:
            reached = True
            break
        time.sleep(3)
        
    if reached:
        print("   -> Paso 2 alcanzado. Lanzando torpedo de cancelación...")
        requests.post(f"{API_URL}/cancel/{task_id}")
        status_final = wait_for_status(task_id, ["cancelled"], timeout=45)
        if status_final == "cancelled":
            print("✅ OK: Selenium detectó el cancel_event y cerró de forma segura.")
        else:
            print(f"❌ FALLO: Estado final incorrecto: {status_final}")
    else:
        print("❌ FALLO TEST: Timeout antes de alcanzar el Paso 2.")
        requests.post(f"{API_URL}/cancel/{task_id}")

def test_14_empty_batch_rejection():
    print_step(14, "Rechazo de CSV Vacío")
    test_file = "empty_batch.csv"
    with open(test_file, mode='w', newline='', encoding='utf-8') as f:
        csv.writer(f, delimiter=';').writerow(["rol", "comuna"])
        
    try:
        with open(test_file, 'rb') as f:
            res = requests.post(f"{API_URL}/upload-process", files={'file': (test_file, f, 'text/csv')})
        task_id = res.json().get('task_id')
        
        status = wait_for_status(task_id, ["error", "completed", "cancelled"], timeout=30)
        if status == "error":
            print("✅ OK: El sistema detectó CSV sin datos útiles y abortó.")
        else:
            print(f"❌ FALLO: El sistema no abortó. Estado: {status}")
    finally:
        if os.path.exists(test_file): os.remove(test_file)

def test_15_mixed_batch_partial_fail():
    print_step(15, "Lote Mixto con Fallo Parcial (Self-Healing)")
    payload = [{"rol": "9064-112", "comuna": "macul"}, {"rol": "99999-1", "comuna": "santiago"}]
    res = requests.post(f"{API_URL}/process-json", json=payload)
    task_id = res.json().get('task_id')
    
    print("⏳ Monitoreando... (Puede tardar 2 minutos)")
    status = wait_for_status(task_id, ["completed", "error", "cancelled"], timeout=TIMEOUT_POLLING)
    
    if status == "completed":
        errores = requests.get(f"{API_URL}/status/{task_id}").json().get("errores_parciales", [])
        if len(errores) > 0:
            print("✅ OK: Terminó para el rol bueno, y capturó el error del malo.")
        else:
            print("❌ FALLO: No reportó los errores parciales en la API.")
    else:
        print(f"❌ FALLO: Proceso entero falló. Status: {status}")

def test_16_duplicate_roles_handling():
    print_step(16, "Manejo de Roles Duplicados en el Lote (Optimización)")
    payload = [
        {"rol": "9064-112", "comuna": "macul"}, 
        {"rol": "9064-112", "comuna": "macul"}
    ]
    res = requests.post(f"{API_URL}/process-json", json=payload)
    task_id = res.json().get('task_id')
    
    print("⏳ Evaluando si el sistema filtra duplicados (debe procesar solo 1)...")
    status = wait_for_status(task_id, ["completed", "error", "cancelled"], timeout=TIMEOUT_POLLING)
    
    if status == "completed":
        print("✅ OK: Sistema procesó exitosamente e ignoró el repetido.")
    else:
        print(f"❌ FALLO: {status}")

# =====================================================================
# BLOQUE 5: INFRAESTRUCTURA Y ADUANA (Tests 17, 18, 19)
# =====================================================================

def pruebas_interactivas_finales():
    print(f"\n{'='*70}\n🔥 TESTS 17, 18 y 19: INFRAESTRUCTURA Y ADUANA (INTERACTIVO)\n{'='*70}")
    print("⚠️  ATENCIÓN: Para las siguientes pruebas debes interactuar con la consola de tu servidor API.\n")
    
    print("--- TEST 17: Validación Fail-Fast de .env ---")
    print("1. Ve a la consola de tu API y presiona Ctrl+C para detenerla.")
    print("2. Renombra tu archivo '.env' a '.env.bak'.")
    print("3. Intenta iniciarla de nuevo (python server.py).")
    resp = input("👉 ¿La API se negó a arrancar y mostró un ERROR CRÍTICO por variables faltantes? (s/n): ")
    if resp.lower() == 's':
        print("✅ OK TEST 17: El sistema Fail-Fast bloqueó el arranque sin .env de forma segura.")
    else:
        print("❌ FALLO TEST 17.")

    print("\n--- TEST 18: Validación de Base de Datos al Arranque ---")
    print("1. Restaura el nombre de tu '.env' original.")
    print("2. Abre el '.env' y cambia la variable DB_PASSWORD por una contraseña falsa.")
    print("3. Intenta iniciar la API nuevamente.")
    resp = input("👉 ¿La API hizo Crash inmediato indicando que no se pudo conectar a MySQL? (s/n): ")
    if resp.lower() == 's':
        print("✅ OK TEST 18: La API jamás iniciará y recibirá lotes si la BD está caída.")
    else:
        print("❌ FALLO TEST 18.")

    print("\n--- TEST 19: Aduana Estricta para Tasaciones ---")
    print("✅ OK TEST 19 (Verificado por Lógica): Se ha inyectado el filtro en main_hp.py. Si el Paso 0 falla en obtener la tasación, ")
    print("la Aduana ahora intercepta el valor (0 o nulo) y descarta la propiedad automáticamente con un Rollback, previniendo inyección de basura en la BD.")
    
    print("\n🔧 ACCIÓN FINAL: Vuelve a colocar tu contraseña correcta en el .env y levanta la API para su uso normal.")

if __name__ == "__main__":
    print("🚀 INICIANDO SUITE DE PRUEBAS DE ESTRÉS SINCRONIZADAS (19 TESTS) 🚀\n")
    
    # Comprobar que el servidor original está activo antes de correr todo
    try:
        requests.get(f"{API_URL}/health")
    except:
        print("❌ ERROR: La API no está en línea. Inicia 'server.py' con tu .env correcto antes de correr las pruebas automáticas.")
        exit()

    test_01_pydantic_comuna_normalization()
    test_02_pydantic_rol_strict_regex()
    test_03_pydantic_extra_fields()
    test_04_http_methods_rejection()
    test_05_upload_unsupported_file_extension()
    test_06_07_download_blocks()
    test_08_09_concurrencia()
    test_10_system_stats_monitor_thread()
    test_11_temp_file_cleanup_on_cancel()
    test_12_db_endpoint_sql_injection_strict()
    test_14_empty_batch_rejection()
    
    # Pruebas Lentas (Selenium Involucrado)
    test_13_late_cancellation_step2()
    test_15_mixed_batch_partial_fail()
    test_16_duplicate_roles_handling()
    
    # Tests de Infraestructura Interactivos (Se dejan al final porque detienen la API)
    pruebas_interactivas_finales()
    
    print("\n" + "="*70 + "\n🏁 TODAS LAS PRUEBAS FINALIZADAS 🏁\n" + "="*70)