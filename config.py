"""
Configuración del proyecto ETL
==============================
Centraliza todos los parámetros de conexión y configuración.

IMPORTANTE: En producción, usar variables de entorno o un gestor de secretos.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class MySQLConfig:
    """Configuración de conexión a MySQL"""
    host: str = "localhost"
    port: int = 3306
    user: str = "etl_user"
    password: str = "etl_password_2024"
    database: str = "etl_ventas"
    charset: str = "utf8mb4"
    
    def get_connection_dict(self) -> dict:
        """Retorna diccionario de conexión para mysql.connector"""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
            "use_pure": True,  # Usar implementación Python pura
            "autocommit": False  # Control manual de transacciones
        }
    
    def get_connection_string(self) -> str:
        """Retorna string de conexión (para SQLAlchemy si se necesita)"""
        return f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class ETLConfig:
    """Configuración general del ETL"""
    # Rutas de archivos
    data_dir: str = "data"
    logs_dir: str = "logs"
    sql_dir: str = "sql"
    
    # Archivos específicos
    csv_filename: str = "ventas_raw.csv"
    log_filename: str = "etl.log"
    
    # Parámetros del ETL
    batch_size: int = 1000  # Registros por batch en la carga
    
    @property
    def csv_path(self) -> str:
        return os.path.join(self.data_dir, self.csv_filename)
    
    @property
    def log_path(self) -> str:
        return os.path.join(self.logs_dir, self.log_filename)


@dataclass
class SyntheticDataConfig:
    """Configuración para generación de datos sintéticos"""
    num_records: int = 10000
    random_seed: int = 42
    
    # Productos disponibles
    productos: tuple = (
        "Laptop HP", "Laptop Dell", "Laptop Lenovo",
        "Monitor Samsung", "Monitor LG", "Monitor ASUS",
        "Teclado Logitech", "Teclado Razer", "Mouse Logitech",
        "Mouse Razer", "Auriculares Sony", "Auriculares Bose",
        "Webcam Logitech", "SSD Samsung", "SSD Kingston",
        "RAM Corsair", "Impresora HP", "Impresora Epson"
    )
    
    # Categorías
    categorias: tuple = (
        "Laptops", "Monitores", "Periféricos", 
        "Almacenamiento", "Componentes", "Impresoras"
    )
    
    # Regiones
    regiones: tuple = (
        "Lima Norte", "Lima Sur", "Lima Centro", "Lima Este",
        "Arequipa", "Trujillo", "Cusco", "Piura", "Chiclayo"
    )


# Instancias globales de configuración
mysql_config = MySQLConfig()
etl_config = ETLConfig()
synthetic_config = SyntheticDataConfig()


# Función para cargar desde variables de entorno (producción)
def load_from_env() -> MySQLConfig:
    """
    Carga configuración desde variables de entorno.
    Útil para entornos de producción.
    
    Variables requeridas:
    - MYSQL_HOST
    - MYSQL_PORT
    - MYSQL_USER
    - MYSQL_PASSWORD
    - MYSQL_DATABASE
    """
    from dotenv import load_dotenv
    load_dotenv()
    
    return MySQLConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "etl_user"),
        password=os.getenv("MYSQL_PASSWORD", "etl_password_2024"),
        database=os.getenv("MYSQL_DATABASE", "etl_ventas")
    )


if __name__ == "__main__":
    # Test de configuración
    print("=== Configuración MySQL ===")
    print(f"Host: {mysql_config.host}")
    print(f"Port: {mysql_config.port}")
    print(f"User: {mysql_config.user}")
    print(f"Database: {mysql_config.database}")
    print(f"\nConnection String: {mysql_config.get_connection_string()}")
    
    print("\n=== Configuración ETL ===")
    print(f"CSV Path: {etl_config.csv_path}")
    print(f"Batch Size: {etl_config.batch_size}")
    
    print("\n=== Configuración Datos Sintéticos ===")
    print(f"Número de registros: {synthetic_config.num_records}")
