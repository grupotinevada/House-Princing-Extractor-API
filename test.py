import requests
import time
import sys

# Configuración
API_URL = "http://localhost:8181"

def print_step(msg):
    print(f"\n{'-'*50}\n▶ {msg}\n{'-'*50}")

def test_health():
    print_step("TEST 1: Verificando conexión (Health Check)")
    try:
        res = requests.get(f"{API_URL}/health")
        if res.status_code == 200:
            print("✅ API Online:", res.json())
            return True
        else:
            print("❌ API respondió con error:", res.status_code)
            return False
    except Exception as e:
        print("❌ No se pudo conectar a la API. ¿Está corriendo server.py?")
        return False

def test_validacion_errores():
    print_step("TEST 2: Verificando filtros de validación (Fail Fast)")
    
    # Mandamos datos intencionalmente malos
    payload_malo = [
        {"rol": "123456", "comuna": "Disney"} # Sin guion y comuna inventada
    ]
    
    res = requests.post(f"{API_URL}/process-json", json=payload_malo)
    
    if res.status_code == 422:
        print("✅ ÉXITO: La API bloqueó la petición correctamente (Error 422).")
        errores = res.json().get('detail', [])
        for err in errores:
            campo = err['loc'][-1]
            mensaje = err['msg']
            print(f"   -> El campo '{campo}' falló: {mensaje}")
    else:
        print(f"❌ FALLO: La API no bloqueó los datos malos. Status: {res.status_code}")

def test_flujo_completo():
    print_step("TEST 3: Ejecutando flujo completo (Polling)")
    
    # Mandamos datos válidos (puedes cambiarlos por roles de prueba reales)
    payload_bueno = [
        {"rol": "9064-112", "comuna": "Macúl"}, # Comuna con tilde para probar la jugera
        {"rol": "3906-209", "comuna": "la serena"} # En minúsculas
    ]
    
    res = requests.post(f"{API_URL}/process-json", json=payload_bueno)
    
    if res.status_code != 200:
        print("❌ FALLO al iniciar proceso:", res.text)
        return

    datos = res.json()
    task_id = datos.get("task_id")
    print(f"✅ Proceso iniciado con Task ID: {task_id}")
    
    # Iniciar Polling (Consultar estado cada 5 segundos)
    print("⏳ Iniciando monitoreo de progreso...")
    
    while True:
        status_res = requests.get(f"{API_URL}/status/{task_id}")
        if status_res.status_code != 200:
            print("❌ Error consultando estado.")
            break
            
        estado = status_res.json()
        progreso = estado.get("progress", 0)
        mensaje = estado.get("message", "")
        status_actual = estado.get("status", "")
        errores_parciales = estado.get("errores_parciales", [])
        
        print(f"   [{progreso}%] {mensaje}")
        
        if status_actual == "completed":
            print("\n🎉 PROCESO COMPLETADO EXITOSAMENTE")
            if errores_parciales:
                print("   ⚠️ Hubo errores parciales en algunas propiedades:")
                for ep in errores_parciales:
                    print(f"      - Rol {ep.get('rol')}: {ep.get('motivo')} (Paso: {ep.get('paso')})")
            
            print(f"\n📥 Puedes descargar el resultado en: {API_URL}/download/{task_id}")
            break
            
        elif status_actual == "error":
            print(f"\n💀 PROCESO TERMINÓ CON ERROR CRÍTICO: {mensaje}")
            break
            
        elif status_actual == "cancelled":
            print("\n🛑 PROCESO FUE CANCELADO.")
            break
            
        time.sleep(5) # Esperar 5 segundos antes de volver a preguntar

if __name__ == "__main__":
    if test_health():
        time.sleep(1)
        test_validacion_errores()
        time.sleep(1)
        test_flujo_completo()