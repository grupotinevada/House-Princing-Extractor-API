<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>House Pricing - Panel de Control API</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.0/font/bootstrap-icons.css">
    
    <style>
        body { background-color: #f8f9fa; font-family: 'Segoe UI', sans-serif; }
        
        /* MODIFICACIÓN: Ancho aumentado al 95% para maximizar espacio */
        .main-card { max-width: 95%; margin: 20px auto; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); border: none; }
        
        .card-header { background: #fff; border-bottom: 1px solid #eee; padding: 20px 30px; border-radius: 15px 15px 0 0 !important; }
        .logo-area { display: flex; align-items: center; gap: 15px; }
        .status-badge { font-size: 0.85em; padding: 5px 12px; border-radius: 20px; }
        
        /* Consola de Logs */
        .console-box {
            background: #1e1e1e; color: #00ff00; font-family: 'Consolas', monospace;
            padding: 15px; border-radius: 8px; height: 300px; overflow-y: auto;
            font-size: 0.9em; margin-top: 20px; border: 1px solid #333;
        }
        .log-entry { margin-bottom: 4px; border-bottom: 1px solid #333; padding-bottom: 2px; }
        .log-time { color: #888; margin-right: 10px; }
        .log-info { color: #4db8ff; }
        .log-success { color: #28a745; }
        .log-warning { color: #ffc107; }
        .log-error { color: #dc3545; }

        /* Arrastrar y Soltar */
        .drop-zone {
            border: 2px dashed #ccc; border-radius: 10px; padding: 40px;
            text-align: center; color: #777; transition: all 0.3s;
            cursor: pointer; background: #fff;
        }
        .drop-zone:hover, .drop-zone.dragover { border-color: #0d6efd; background: #f1f8ff; color: #0d6efd; }
    </style>
</head>
<body>

<div class="container-fluid"> <div class="card main-card">
        <div class="card-header d-flex justify-content-between align-items-center">
            <div class="logo-area">
                <i class="bi bi-buildings-fill text-primary fs-2"></i>
                <div>
                    <h4 class="mb-0 fw-bold">Grupo House</h4>
                    <small class="text-muted">Cliente API v2.0</small>
                </div>
            </div>
            <div id="api-status-badge" class="badge bg-secondary status-badge">
                <i class="bi bi-circle-fill" style="font-size: 8px;"></i> Conectando...
            </div>
        </div>

        <div class="card-body p-4">
            
            <ul class="nav nav-tabs mb-4" id="myTab" role="tablist">
                <li class="nav-item" role="presentation">
                    <button class="nav-link active" id="proceso-tab" data-bs-toggle="tab" data-bs-target="#panel-proceso" type="button" role="tab">
                        <i class="bi bi-cpu"></i> Procesamiento
                    </button>
                </li>
                <li class="nav-item" role="presentation">
                    <button class="nav-link" id="datos-tab" data-bs-toggle="tab" data-bs-target="#panel-datos" type="button" role="tab">
                        <i class="bi bi-database"></i> Visualizador BD
                    </button>
                </li>
                <li class="nav-item" role="presentation">
                    <button class="nav-link" id="manual-tab" data-bs-toggle="tab" data-bs-target="#panel-manual" type="button" role="tab">
                        <i class="bi bi-list-check"></i> Selección Manual
                    </button>
                </li>
            </ul>

            <div class="tab-content" id="myTabContent">
                
                <div class="tab-pane fade show active" id="panel-proceso" role="tabpanel">
                    <div id="upload-section">
                        <h6 class="fw-bold mb-3"><i class="bi bi-1-circle"></i> Cargar Archivo de Propiedades</h6>
                        <div class="drop-zone" id="drop-zone" onclick="document.getElementById('fileInput').click()">
                            <i class="bi bi-cloud-arrow-up fs-1 mb-2"></i>
                            <p class="mb-0">Arrastra tu Excel/CSV aquí o haz clic para buscar</p>
                            <small class="text-muted">Formatos soportados: .xlsx, .xls, .csv</small>
                            <input type="file" id="fileInput" hidden accept=".xlsx, .xls, .csv">
                        </div>
                        <div id="file-info" class="alert alert-info mt-3 d-none d-flex justify-content-between align-items-center">
                            <span><i class="bi bi-file-earmark-excel-fill"></i> <strong id="filename-display">archivo.xlsx</strong></span>
                            <button class="btn btn-sm btn-outline-danger" onclick="resetFile()"><i class="bi bi-x-lg"></i></button>
                        </div>
                    </div>

                    <div class="mt-4">
                        <div class="d-flex gap-2">
                            <button id="btn-start" class="btn btn-primary px-4" disabled onclick="iniciarProceso()">
                                <i class="bi bi-play-fill"></i> Iniciar Proceso
                            </button>
                            <button id="btn-cancel" class="btn btn-danger" disabled onclick="cancelarProceso()">
                                <i class="bi bi-stop-fill"></i> Cancelar
                            </button>
                            <a id="btn-download" href="#" class="btn btn-success ms-auto d-none" target="_blank">
                                <i class="bi bi-download"></i> Descargar Excel
                            </a>
                        </div>

                        <div class="mt-4">
                            <div class="d-flex justify-content-between mb-1">
                                <span id="progress-text" class="fw-bold text-muted">Esperando inicio...</span>
                                <span id="progress-percent" class="fw-bold text-primary">0%</span>
                            </div>
                            <div class="progress" style="height: 20px;">
                                <div id="progress-bar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%"></div>
                            </div>
                        </div>
                    </div>

                    <div class="console-box" id="console">
                        <div class="log-entry"><span class="log-time">[System]</span> Listo para operar.</div>
                    </div>
                </div>

                <div class="tab-pane fade" id="panel-datos" role="tabpanel">
                    <div class="row">
                        <div class="col-md-2 border-end">
                            <h6 class="fw-bold mb-3 text-muted">Tablas</h6>
                            <div class="list-group list-group-flush" id="lista-tablas">
                                <button type="button" class="list-group-item list-group-item-action text-truncate" onclick="cargarTabla('propiedades', this)" title="propiedades">propiedades</button>
                                <button type="button" class="list-group-item list-group-item-action text-truncate" onclick="cargarTabla('construcciones', this)" title="construcciones">construcciones</button>
                                <button type="button" class="list-group-item list-group-item-action text-truncate" onclick="cargarTabla('roles_asociados', this)" title="roles_asociados">roles_asociados</button>
                                <button type="button" class="list-group-item list-group-item-action text-truncate" onclick="cargarTabla('deudas_tgr', this)" title="deudas_tgr">deudas_tgr</button>
                                <button type="button" class="list-group-item list-group-item-action text-truncate" onclick="cargarTabla('comparables', this)" title="comparables">comparables</button>
                            </div>
                        </div>
                        
                        <div class="col-md-10">
                            <div class="d-flex justify-content-between align-items-center mb-3">
                                <h5 class="mb-0" id="titulo-tabla"><i class="bi bi-table"></i> Selecciona una tabla</h5>
                                <small class="text-muted">Últimos 100 registros</small>
                            </div>
                            
                            <div id="loader-tabla" class="text-center py-5 d-none">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2 text-muted">Cargando datos...</p>
                            </div>

                            <div class="table-responsive border rounded" style="max-height: 70vh; overflow-y: auto;">
                                <table class="table table-striped table-hover mb-0 font-monospace" style="font-size: 0.85em;">
                                    <thead class="table-dark sticky-top" id="tabla-head">
                                        </thead>
                                    <tbody id="tabla-body">
                                        <tr><td class="text-center p-4 text-muted">Esperando selección...</td></tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="tab-pane fade" id="panel-manual" role="tabpanel">
    <h6 class="fw-bold mb-3"><i class="bi bi-1-circle"></i> Seleccionar Propiedades de Prueba</h6>
    <div class="table-responsive border rounded bg-white">
        <table class="table table-hover align-middle mb-0">
            <thead class="table-light">
                <tr>
                    <th style="width: 40px;"><input type="checkbox" id="check-all" onclick="toggleAllChecks(this)"></th>
                    <th>Rol</th>
                    <th>Comuna</th>
                    <th>Dirección</th>
                </tr>
            </thead>
            <tbody id="lista-manual-body">
                <tr>
                    <td><input type="checkbox" class="prop-check" data-rol="3906-209" data-comuna="La Serena" data-dir="AV CENTRAL EDIF 1 5000 DP 1104"></td>
                    <td>3906-209</td><td>La Serena</td><td>AV CENTRAL EDIF 1 5000 DP 1104</td>
                </tr>
                <tr>
                    <td><input type="checkbox" class="prop-check" data-rol="9064-112" data-comuna="Macul" data-dir="AV. A. VESPUCIO 4455 DP 1109 D"></td>
                    <td>9064-112</td><td>Macul</td><td>AV. A. VESPUCIO 4455 DP 1109 D</td>
                </tr>
                <tr>
                    <td><input type="checkbox" class="prop-check" data-rol="4560-16" data-comuna="Temuco" data-dir="Las Marantas 02361"></td>
                    <td>4560-16</td><td>Temuco</td><td>Las Marantas 02361</td>
                </tr>
                <tr>
                    <td><input type="checkbox" class="prop-check" data-rol="1825-145" data-comuna="Providencia" data-dir="Dario Urzua 1963"></td>
                    <td>1825-145</td><td>Providencia</td><td>Dario Urzua 1963</td>
                </tr>
            </tbody>
        </table>
    </div>
    <div class="mt-3">
        <button class="btn btn-primary" onclick="iniciarProcesoManual()">
            <i class="bi bi-play-circle"></i> Procesar Seleccionados
        </button>
    </div>
</div>

            </div> </div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>

<script>
    // =========================================================================
    // CONFIGURACIÓN DE CONEXIÓN API
    // =========================================================================
    IP_ADDRESS=""
    // const API_URL = "https://api_hp.inevada.cl"; 
    const API_URL = "http://localhost:8181"; 
    
    // =========================================================================

    let currentTaskId = null;
    let pollInterval = null;

    console.log("Configurado para conectar a API en:", API_URL);

    // --- 1. GESTIÓN DE ARCHIVOS ---
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('fileInput');

    dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('dragover'); });
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
    });

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length) handleFile(e.target.files[0]);
    });

    function handleFile(file) {
        document.getElementById('filename-display').innerText = file.name;
        document.getElementById('file-info').classList.remove('d-none');
        document.getElementById('drop-zone').classList.add('d-none');
        document.getElementById('btn-start').disabled = false;
        log(`Archivo cargado: ${file.name}`, 'info');
    }

    function resetFile() {
        fileInput.value = '';
        document.getElementById('file-info').classList.add('d-none');
        document.getElementById('drop-zone').classList.remove('d-none');
        document.getElementById('btn-start').disabled = true;
        log("Archivo removido.", 'warning');
    }

    // --- 2. COMUNICACIÓN API ---

    // Chequeo de Salud inicial
    async function checkHealth() {
        const badge = document.getElementById('api-status-badge');
        try {
            const res = await fetch(`${API_URL}/health`);
            if (res.ok) {
                badge.className = 'badge bg-success status-badge';
                badge.innerHTML = '<i class="bi bi-check-circle-fill"></i> API Online';
                log(`Conexión establecida con API (${API_URL})`, "success");
            } else { throw new Error(); }
        } catch (e) {
            badge.className = 'badge bg-danger status-badge';
            badge.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i> API Offline';
            log("No se puede conectar a la API. Verifica la IP en el código PHP.", "error");
        }
    }
    checkHealth();

    // Iniciar Proceso (Upload)
    async function iniciarProceso() {
        const file = fileInput.files[0];
        if (!file) return;

        // UI Updates
        document.getElementById('btn-start').disabled = true;
        document.getElementById('btn-cancel').disabled = false;
        document.getElementById('btn-download').classList.add('d-none');
        resetProgress();
        
        const formData = new FormData();
        formData.append("file", file);

        log("Subiendo archivo e iniciando proceso...", "info");

        try {
            const res = await fetch(`${API_URL}/upload-process`, { method: 'POST', body: formData });
            const data = await res.json();

            if (res.ok) {
                currentTaskId = data.task_id;
                log(`Tarea iniciada ID: ${currentTaskId}`, "success");
                // Iniciar Polling
                pollInterval = setInterval(checkStatus, 2000); // Cada 2 segundos
            } else {
                log(`Error al iniciar: ${data.detail}`, "error");
                document.getElementById('btn-start').disabled = false;
            }
        } catch (e) {
            log(`Error de red: ${e.message}`, "error");
            document.getElementById('btn-start').disabled = false;
        }
    }

    // Consultar Estado (Polling)
    async function checkStatus() {
        if (!currentTaskId) return;

        try {
            const res = await fetch(`${API_URL}/status/${currentTaskId}`);
            const data = await res.json();

            if (res.ok) {
                updateProgress(data.progress, data.message);

                if (data.status === 'completed') {
                    finalizarProceso(true, data.message);
                } else if (data.status === 'error') {
                    finalizarProceso(false, data.message);
                } else if (data.status === 'cancelled') {
                    finalizarProceso(false, "Proceso cancelado por el usuario.");
                }
            }
        } catch (e) {
            log(`Perdida de conexión con API... reintentando`, "warning");
        }
    }

    // Cancelar
    async function cancelarProceso() {
        if (!currentTaskId) return;
        if (!confirm("¿Seguro que deseas detener el proceso?")) return;

        try {
            log("Enviando señal de cancelación...", "warning");
            await fetch(`${API_URL}/cancel/${currentTaskId}`, { method: 'POST' });
            document.getElementById('btn-cancel').disabled = true;
        } catch (e) {
            log(`Error al cancelar: ${e.message}`, "error");
        }
    }

    // Finalización
    function finalizarProceso(exito, mensaje) {
        clearInterval(pollInterval);
        pollInterval = null;
        document.getElementById('btn-start').disabled = false;
        document.getElementById('btn-cancel').disabled = true;

        if (exito) {
            log(`¡PROCESO TERMINADO! ${mensaje}`, "success");
            updateProgress(100, "Completado");
            
            // Habilitar Descarga
            const btnDownload = document.getElementById('btn-download');
            btnDownload.href = `${API_URL}/download/${currentTaskId}`;
            btnDownload.classList.remove('d-none');
        } else {
            log(`Proceso detenido: ${mensaje}`, "error");
            document.getElementById('progress-bar').classList.add('bg-danger');
        }
    }

    // --- UTILS UI ---
    function updateProgress(percent, msg) {
        const bar = document.getElementById('progress-bar');
        const txtPercent = document.getElementById('progress-percent');
        const txtStatus = document.getElementById('progress-text');

        // Limpieza básica del mensaje si es muy largo
        const shortMsg = msg.length > 60 ? msg.substring(0, 60) + "..." : msg;

        bar.style.width = `${percent}%`;
        txtPercent.innerText = `${percent}%`;
        txtStatus.innerText = shortMsg;
    }

    function resetProgress() {
        const bar = document.getElementById('progress-bar');
        bar.style.width = '0%';
        bar.className = 'progress-bar progress-bar-striped progress-bar-animated';
        bar.classList.remove('bg-danger');
        document.getElementById('progress-percent').innerText = '0%';
        document.getElementById('progress-text').innerText = 'Preparando...';
    }

    function log(msg, type = 'info') {
        const consoleBox = document.getElementById('console');
        const now = new Date().toLocaleTimeString();
        const entry = document.createElement('div');
        entry.className = 'log-entry';
        
        let colorClass = 'log-info';
        if (type === 'success') colorClass = 'log-success';
        if (type === 'warning') colorClass = 'log-warning';
        if (type === 'error') colorClass = 'log-error';

        entry.innerHTML = `<span class="log-time">[${now}]</span> <span class="${colorClass}">${msg}</span>`;
        consoleBox.appendChild(entry);
        consoleBox.scrollTop = consoleBox.scrollHeight;
    }

    // =========================================================================
    // LÓGICA DE TABLAS
    // =========================================================================
    
    async function cargarTabla(nombreTabla, btnElement) {
        const buttons = document.querySelectorAll('#lista-tablas button');
        buttons.forEach(b => b.classList.remove('active'));
        if(btnElement) btnElement.classList.add('active');

        document.getElementById('titulo-tabla').innerHTML = `<i class="bi bi-table"></i> Tabla: <b>${nombreTabla}</b>`;
        document.getElementById('loader-tabla').classList.remove('d-none');
        const thead = document.getElementById('tabla-head');
        const tbody = document.getElementById('tabla-body');
        thead.innerHTML = '';
        tbody.innerHTML = ''; 

        try {
            const res = await fetch(`${API_URL}/api/datos/${nombreTabla}`);
            const data = await res.json();

            if (!res.ok) throw new Error(data.detail || "Error desconocido al cargar tabla");

            if (data.length === 0) {
                tbody.innerHTML = '<tr><td class="text-center p-4">La tabla está vacía.</td></tr>';
            } else {
                const columns = Object.keys(data[0]);
                let headerRow = '<tr>';
                columns.forEach(col => {
                    headerRow += `<th scope="col">${col}</th>`;
                });
                headerRow += '</tr>';
                thead.innerHTML = headerRow;

                let bodyContent = '';
                data.forEach(row => {
                    bodyContent += '<tr>';
                    columns.forEach(col => {
                        let val = row[col];
                        if(val === null) val = '<span class="text-muted">null</span>';
                        bodyContent += `<td>${val}</td>`;
                    });
                    bodyContent += '</tr>';
                });
                tbody.innerHTML = bodyContent;
            }

        } catch (error) {
            tbody.innerHTML = `<tr><td class="text-danger p-4">Error: ${error.message}</td></tr>`;
        } finally {
            document.getElementById('loader-tabla').classList.add('d-none');
        }
    }
    function toggleAllChecks(source) {
    const checkboxes = document.querySelectorAll('.prop-check');
    checkboxes.forEach(cb => cb.checked = source.checked);
}

async function iniciarProcesoManual() {
    const selected = [];
    document.querySelectorAll('.prop-check:checked').forEach(cb => {
        selected.push({
            rol: cb.dataset.rol,
            comuna: cb.dataset.comuna,
            direccion: cb.dataset.dir
        });
    });

    if (selected.length === 0) {
        alert("Por favor selecciona al menos una propiedad.");
        return;
    }

    log(`Iniciando proceso manual con ${selected.length} propiedades...`, "info");
    
    // Cambiar visualmente al tab de procesamiento para ver los logs y progreso
    const triggerEl = document.querySelector('#proceso-tab');
    bootstrap.Tab.getInstance(triggerEl).show();

    try {
        const res = await fetch(`${API_URL}/process-json`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(selected)
        });
        const data = await res.json();

        if (res.ok) {
            currentTaskId = data.task_id;
            document.getElementById('btn-cancel').disabled = false;
            pollInterval = setInterval(checkStatus, 2000);
            log(`Tarea manual iniciada: ${currentTaskId}`, "success");
        } else {
            log(`Error: ${data.detail}`, "error");
        }
    } catch (e) {
        log(`Error de red: ${e.message}`, "error");
    }
}

</script>

</body>
</html>