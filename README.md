# ETL Python: CSV → MySQL

## Proyecto de Data Engineering
**Caso práctico de ETL local con Python y MySQL**

---

## 📋 Descripción

Este proyecto implementa un pipeline ETL completo que:
- **Extract**: Lee datos de un archivo CSV sintético
- **Transform**: Aplica limpieza, validaciones y transformaciones
- **Load**: Carga los datos procesados a una base de datos MySQL local

---

## 🛠️ Requisitos Previos

### 1. Instalar MySQL Server

#### Windows
```bash
# Opción 1: Descargar instalador desde
# https://dev.mysql.com/downloads/mysql/

# Opción 2: Usando Chocolatey
choco install mysql
```

#### macOS
```bash
# Usando Homebrew
brew install mysql
brew services start mysql
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install mysql-server
sudo systemctl start mysql
sudo systemctl enable mysql
```

### 2. Configurar MySQL después de instalación

```bash
# Ejecutar configuración segura
sudo mysql_secure_installation

# Conectarse a MySQL como root
mysql -u root -p
```

### 3. Crear usuario y base de datos para el proyecto

Ejecuta estos comandos dentro de MySQL:

```sql
-- Crear la base de datos
CREATE DATABASE etl_ventas;

-- Crear usuario específico para el proyecto
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'etl_password_2024';

-- Otorgar permisos
GRANT ALL PRIVILEGES ON etl_ventas.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;

-- Verificar
SHOW DATABASES;
SELECT User, Host FROM mysql.user;
```

---

## 🐍 Configuración del Entorno Python

### Crear entorno virtual

```bash
# Crear entorno
python -m venv venv

# Activar (Windows)
venv\Scripts\activate

# Activar (Linux/macOS)
source venv/bin/activate
```

### Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## 📁 Estructura del Proyecto

```
etl_mysql_project/
│
├── README.md                 # Este archivo
├── requirements.txt          # Dependencias Python
├── config.py                 # Configuración de conexión
├── generate_synthetic_data.py # Generador de datos CSV
├── etl_pipeline.py           # Pipeline ETL principal
│
├── data/
│   └── ventas_raw.csv        # Datos sintéticos (generado)
│
├── logs/
│   └── etl.log               # Logs del proceso
│
└── sql/
    └── create_tables.sql     # Scripts DDL
```

---

## 🚀 Ejecución

### Paso 1: Generar datos sintéticos
```bash
python generate_synthetic_data.py
```

### Paso 2: Ejecutar el ETL
```bash
python etl_pipeline.py
```

### Paso 3: Verificar en MySQL
```bash
mysql -u etl_user -p etl_ventas

# Dentro de MySQL
SELECT COUNT(*) FROM ventas;
SELECT * FROM ventas LIMIT 10;
```

---

## 🔧 Troubleshooting

### Error de conexión MySQL
```
mysql.connector.errors.InterfaceError: 2003 (HY000): Can't connect to MySQL server
```
**Solución**: Verificar que MySQL esté corriendo
```bash
# Linux
sudo systemctl status mysql

# macOS
brew services list

# Windows
net start mysql
```

### Error de autenticación
```
Access denied for user 'etl_user'@'localhost'
```
**Solución**: Recrear usuario con contraseña correcta
```sql
DROP USER IF EXISTS 'etl_user'@'localhost';
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'etl_password_2024';
GRANT ALL PRIVILEGES ON etl_ventas.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
```

---

## 📊 Modelo de Datos

### Tabla: `ventas`
| Campo | Tipo | Descripción |
|-------|------|-------------|
| id | INT | Primary Key |
| fecha | DATE | Fecha de venta |
| producto | VARCHAR(100) | Nombre del producto |
| categoria | VARCHAR(50) | Categoría |
| cantidad | INT | Unidades vendidas |
| precio_unitario | DECIMAL(10,2) | Precio por unidad |
| total | DECIMAL(12,2) | Total de la venta |
| cliente_id | VARCHAR(20) | ID del cliente |
| region | VARCHAR(50) | Región geográfica |
| vendedor | VARCHAR(100) | Nombre del vendedor |
| created_at | TIMESTAMP | Fecha de carga |

---

## 👨‍🏫 Autor
Proyecto creado para fines educativos - Data Engineering
