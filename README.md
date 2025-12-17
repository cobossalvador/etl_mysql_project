# 🏗️ Pipeline ETL con Python y MySQL

## 🧠 Descripción
Este proyecto implementa un **pipeline ETL (Extract, Transform, Load)** utilizando **Python y MySQL**, simulando un flujo de datos real bajo buenas prácticas de **Data Engineering**.

El objetivo es extraer datos (reales o sintéticos), transformarlos y cargarlos en una base de datos MySQL, manteniendo separación de responsabilidades, control de configuración, logging y versionado.

El proyecto está orientado a demostrar un **nivel intermedio** de conocimientos en pipelines de datos.

---

## 🏗️ Arquitectura del Pipeline
El flujo del pipeline sigue una estructura clara y desacoplada:

Generación / Fuente de Datos
↓
Extracción (CSV / Datos sintéticos)
↓
Transformación con Python
↓
Carga a MySQL
↓
Persistencia y Logging

yaml
Copiar código

---

## 🚀 Funcionalidades principales
- Generación de datos sintéticos para pruebas
- Extracción de datos desde archivos CSV
- Transformaciones con Python
- Creación de tablas en MySQL mediante scripts SQL
- Carga automatizada de datos a MySQL
- Registro de eventos y errores mediante logging
- Configuración desacoplada mediante archivo de configuración
- Manejo de variables de entorno

---

## 🛠️ Tecnologías utilizadas
- **Python** – desarrollo del pipeline ETL
- **MySQL** – base de datos relacional
- **SQL** – creación y gestión de tablas
- **Visual Studio Code** – entorno de desarrollo
- **Git & GitHub** – control de versiones
- **Virtual Environment (venv)** – aislamiento de dependencias

---

## 📂 Estructura del proyecto

etl_mysql_project/
│
├── data/
│ ├── ventas_raw.csv # Datos de entrada (raw)
│ └── .gitkeep
│
├── logs/
│ ├── etl.log # Logs del pipeline
│ └── .gitkeep
│
├── sql/
│ └── create_tables.sql # Script SQL para crear tablas
│
├── venv/ # Entorno virtual (no versionado)
│
├── config.py # Configuración del proyecto
├── etl_pipeline.py # Script principal del pipeline ETL
├── generate_synthetic_data.py# Generación de datos de prueba
├── requirements.txt # Dependencias del proyecto
├── .env.example # Variables de entorno de ejemplo
└── README.md

yaml
Copiar código

---

## ⚙️ Configuración del entorno

### 1️⃣ Crear entorno virtual
```bash
python -m venv venv
2️⃣ Activar entorno virtual
Windows

bash
Copiar código
venv\Scripts\activate
Linux / Mac

bash
Copiar código
source venv/bin/activate
3️⃣ Instalar dependencias
bash
Copiar código
pip install -r requirements.txt
4️⃣ Configurar variables de entorno
Crear un archivo .env basado en .env.example y completar los valores:

env
Copiar código
DB_HOST=localhost
DB_PORT=3306
DB_NAME=nombre_base_datos
DB_USER=usuario
DB_PASSWORD=password
🗄️ Base de datos MySQL
Antes de ejecutar el pipeline:

Crear la base de datos en MySQL

Ejecutar el script de creación de tablas:

sql
Copiar código
sql/create_tables.sql
▶️ Ejecución del pipeline
Generar datos sintéticos (opcional)
bash
Copiar código
python generate_synthetic_data.py
Ejecutar el pipeline ETL
bash
Copiar código
python etl_pipeline.py
Durante la ejecución:

Los datos son procesados y cargados en MySQL

Los eventos y errores quedan registrados en logs/etl.log

📊 Logging y monitoreo
El pipeline implementa logging para:

Inicio y fin del proceso ETL

Errores de conexión

Fallos en transformación o carga

Validaciones básicas

Esto permite trazabilidad y facilita el debugging del proceso.

📈 Aprendizajes y buenas prácticas
Diseño de pipelines ETL desacoplados

Uso de scripts SQL para control de esquema

Manejo de variables de entorno

Implementación de logging en procesos de datos

Separación entre datos, lógica y configuración

Simulación de flujos reales de Data Engineering

🎯 Enfoque profesional
Este proyecto está orientado a roles como:

Data Engineer

Analista de Datos Técnico

Desarrollador Python enfocado en datos

Presenta un enfoque práctico y realista, más allá de ejercicios académicos.
