# 🏠 House Pricing Automation API

* [1. Introducción](#1-introducción)
* [2. Arquitectura](#2-arquitectura-del-sistema)
* [3. Estructura de Archivos](#3-estructura-de-archivos)
* [4. Requisitos Linux](#4-requisitos-del-servidor-linux)
* [5. Instalación](#5-guía-de-instalación)
* [6. Pipeline de Lógica](#6-pipeline-de-lógica-core)
* [7. Referencia API](#7-referencia-de-api)
* [8. Integración PHP](#8-integración-frontend-php)

---

## 📘 Documentación Técnica

**House Pricing Automation API** es una solución desacoplada diseñada para procesar masivamente tasaciones inmobiliarias.
Separa claramente:

* 🖥️ **Interfaz de usuario (PHP)**
* ⚙️ **Procesamiento pesado (Python + Selenium)**
* 🗄️ **Persistencia de datos (MySQL)**

Esto permite evitar *timeouts*, mejorar la concurrencia y escalar el procesamiento.

El sistema acepta:

* 📄 Subida de archivos **Excel / CSV**
* 📦 Envío de **JSON** con roles y comunas

Como resultado genera:

* 📑 Reportes enriquecidos con:

  * OCR de PDFs (Informe de Antecedentes)
  * Comparables de mercado (Web Scraping)
* 🗃️ Inserción automática en base de datos

---

## 🏗️ 2. Arquitectura del Sistema

Modelo **Cliente – Servidor Asíncrono** con control de estado y persistencia en base de datos.

```
[ Cliente PHP ]
       |
       v
[ FastAPI Backend ]
       |
       v
[ Worker Python ]
       |
       v
[ MySQL ]
```

Flujo de ejecución:

1. Subida de archivo o JSON
2. Generación de `task_id` (UUID)
3. Ejecución de Pasos 0 a 4
4. Inserción de datos en BD
5. Cliente consulta estado (polling)
6. API responde progreso
7. Cliente descarga archivo final
8. API sirve el resultado

### 🔩 Componentes

* **PHP (Frontend)**
  Maneja la interfaz y llamadas HTTP. No procesa datos.

* **FastAPI (Backend)**
  Orquestador de tareas. Usa `BackgroundTasks`.

* **Pipeline (Core Python)**
  Implementa la lógica de negocio.

* **MySQL**
  Almacena propiedades, roles, construcciones y comparables.

---

## 🗂️ 3. Estructura de Archivos

/proyecto_root/
│
├── server.py
├── requirements.txt
├── .env
├── logger.py
│
├── api/
│   ├── **init**.py
│   ├── main_hp.py
│   ├── paso0_hp.py
│   ├── paso1_hp.py
│   ├── paso2_hp.py
│   ├── paso3_hp.py
│   └── paso4_hp.py
│
├── uploads/
├── house_pricing_outputs/
└── logs/

---

## 🐧 4. Requisitos del Servidor (Linux)

Sistema recomendado:

* Ubuntu 20.04+
* Fedora 38+

### 🔧 Componentes base

* Python 3.10+
* Google Chrome Stable
* MySQL Server
* Permisos de escritura (uploads, outputs, logs)

**Nota:** Selenium Manager descarga automáticamente `chromedriver`.

---

## ⚙️ 5. Guía de Instalación

### Paso 1: Google Chrome

Instalar la versión estable oficial.

### Paso 2: Entorno Python

* Crear entorno virtual
* Activarlo
* Instalar dependencias desde `requirements.txt`

### Paso 3: Variables de entorno (.env)

Configurar:

* DB_HOST
* DB_USER
* DB_PASSWORD
* DB_NAME

### Paso 4: Ejecución

Iniciar el servidor en el puerto 8181.

---

## 🔁 6. Pipeline de Lógica (Core)

Ubicado en `api/`, con control de progreso.

| Paso | Progreso | Descripción                                          |
| ---- | -------- | ---------------------------------------------------- |
| 0    | 0–25%    | Descarga automática de PDFs (Selenium + paralelismo) |
| 1    | 25–50%   | Extracción de texto y coordenadas (pdfplumber)       |
| 2    | 50–75%   | Obtención de comparables y cálculo de distancias     |
| 3    | 75–90%   | Generación de Excel relacional                       |
| 4    | 90–100%  | Inserción transaccional en MySQL                     |

---

## 🌐 7. Referencia de API

Base URL:
`http://TU_IP:8181`

| Método | Endpoint            | Descripción           |
| ------ | ------------------- | --------------------- |
| GET    | /health             | Estado del sistema    |
| POST   | /upload-process     | Subida de Excel o CSV |
| POST   | /process-json       | Procesa lista JSON    |
| GET    | /status/{task_id}   | Progreso del proceso  |
| POST   | /cancel/{task_id}   | Cancela proceso       |
| GET    | /download/{task_id} | Descarga Excel final  |
| GET    | /api/datos/{tabla}  | Últimos 100 registros |

---

## 🧩 8. Integración Frontend (PHP)

Configuración base para consumir la API desde PHP.

### Parámetro crítico

Definir la URL del backend:

* IP real del servidor
* Puerto 8181

Esto permite que PHP consuma correctamente todos los endpoints.

---

📌 **House Pricing Automation API**
Sistema robusto para tasaciones automatizadas, escalable y desacoplado, diseñado para ambientes productivos con alto volumen de procesamiento.
