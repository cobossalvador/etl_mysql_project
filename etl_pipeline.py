
"""
Pipeline ETL: CSV → MySQL
=========================
Implementa un proceso ETL completo con manejo de errores,
logging, y buenas prácticas de ingeniería de datos.

Uso:
    python etl_pipeline.py

Autor: Proyecto educativo de Data Engineering
"""

import os
import sys
import uuid
import logging
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any
from contextlib import contextmanager

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error as MySQLError

from config import mysql_config, etl_config


# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================

def setup_logging() -> logging.Logger:
    """Configura el sistema de logging"""
    
    # Crear directorio de logs si no existe
    os.makedirs(etl_config.logs_dir, exist_ok=True)
    
    # Configurar logger
    logger = logging.getLogger('ETL_Pipeline')
    logger.setLevel(logging.DEBUG)
    
    # Formato de log
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para archivo
    file_handler = logging.FileHandler(
        etl_config.log_path,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Añadir handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# Logger global
logger = setup_logging()


# ============================================================
# CONEXIÓN A MYSQL
# ============================================================

class MySQLConnection:
    """
    Gestor de conexiones a MySQL con context manager.
    Implementa el patrón de conexión segura.
    """
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establece conexión a MySQL"""
        try:
            logger.info(f"Conectando a MySQL: {self.config['host']}:{self.config['port']}")
            
            self.connection = mysql.connector.connect(**self.config)
            
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                logger.info(f"✅ Conexión exitosa - MySQL Server v{db_info}")
                
                self.cursor = self.connection.cursor(dictionary=True)
                
                # Mostrar base de datos actual
                self.cursor.execute("SELECT DATABASE() as db")
                result = self.cursor.fetchone()
                logger.info(f"   Base de datos: {result['db']}")
                
                return True
                
        except MySQLError as e:
            logger.error(f"❌ Error de conexión MySQL: {e}")
            return False
    
    def disconnect(self):
        """Cierra la conexión de forma segura"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logger.info("Conexión a MySQL cerrada")
        except MySQLError as e:
            logger.warning(f"Error al cerrar conexión: {e}")
    
    def execute(self, query: str, params: tuple = None) -> Any:
        """Ejecuta una query"""
        try:
            self.cursor.execute(query, params)
            return self.cursor
        except MySQLError as e:
            logger.error(f"Error ejecutando query: {e}")
            raise
    
    def executemany(self, query: str, data: List[tuple]) -> int:
        """Ejecuta query para múltiples registros"""
        try:
            self.cursor.executemany(query, data)
            return self.cursor.rowcount
        except MySQLError as e:
            logger.error(f"Error en executemany: {e}")
            raise
    
    def commit(self):
        """Commit de la transacción"""
        self.connection.commit()
    
    def rollback(self):
        """Rollback de la transacción"""
        self.connection.rollback()


@contextmanager
def get_mysql_connection():
    """Context manager para conexiones MySQL"""
    conn = MySQLConnection(mysql_config.get_connection_dict())
    try:
        if not conn.connect():
            raise ConnectionError("No se pudo conectar a MySQL")
        yield conn
    finally:
        conn.disconnect()


# ============================================================
# FASE EXTRACT
# ============================================================

def extract(filepath: str) -> pd.DataFrame:
    """
    EXTRACT: Lee datos del archivo CSV
    
    Args:
        filepath: Ruta al archivo CSV
        
    Returns:
        DataFrame con los datos raw
    """
    logger.info("=" * 60)
    logger.info("FASE: EXTRACT")
    logger.info("=" * 60)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
    
    logger.info(f"Leyendo archivo: {filepath}")
    
    # Leer CSV con configuración adecuada
    df = pd.read_csv(
        filepath,
        encoding='utf-8',
        low_memory=False
    )
    
    # Información del dataset
    file_size = os.path.getsize(filepath) / (1024 * 1024)
    
    logger.info(f"   Registros leídos: {len(df):,}")
    logger.info(f"   Columnas: {len(df.columns)}")
    logger.info(f"   Tamaño archivo: {file_size:.2f} MB")
    logger.info(f"   Columnas: {list(df.columns)}")
    
    # Mostrar tipos de datos originales
    logger.debug("Tipos de datos originales:")
    for col, dtype in df.dtypes.items():
        logger.debug(f"   {col}: {dtype}")
    
    return df


# ============================================================
# FASE TRANSFORM
# ============================================================

class DataTransformer:
    """
    Clase para aplicar transformaciones y limpieza de datos.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.rejected_records = []
        self.stats = {
            'original_count': len(df),
            'nulls_fixed': 0,
            'duplicates_removed': 0,
            'dates_fixed': 0,
            'negatives_fixed': 0,
            'strings_cleaned': 0
        }
    
    def clean_strings(self) -> 'DataTransformer':
        """Limpia campos de texto: espacios y mayúsculas"""
        string_columns = ['producto', 'categoria', 'region', 'vendedor', 'cliente_id']
        
        for col in string_columns:
            if col in self.df.columns:
                # Remover espacios extra
                original_values = self.df[col].copy()
                self.df[col] = self.df[col].astype(str).str.strip()
                
                # Estandarizar capitalización
                if col == 'region':
                    self.df[col] = self.df[col].str.title()
                
                # Contar cambios
                changed = (original_values != self.df[col]).sum()
                self.stats['strings_cleaned'] += changed
        
        logger.info(f"   Strings limpiados: {self.stats['strings_cleaned']}")
        return self
    
    def fix_dates(self) -> 'DataTransformer':
        """Convierte y valida fechas"""
        
        # Intentar convertir a datetime
        def parse_date(x):
            if pd.isna(x):
                return pd.NaT
            try:
                # Intentar formato ISO primero
                return pd.to_datetime(x)
            except:
                try:
                    # Intentar formato dd/mm/yyyy
                    return pd.to_datetime(x, format='%d/%m/%Y')
                except:
                    return pd.NaT
        
        original_nulls = self.df['fecha'].isna().sum()
        self.df['fecha'] = self.df['fecha'].apply(parse_date)
        new_nulls = self.df['fecha'].isna().sum()
        
        self.stats['dates_fixed'] = new_nulls - original_nulls
        logger.info(f"   Fechas convertidas (problemas: {self.stats['dates_fixed']})")
        
        return self
    
    def fix_numeric_values(self) -> 'DataTransformer':
        """Corrige valores numéricos inválidos"""
        
        # Convertir cantidad a numérico y hacer valores positivos
        self.df['cantidad'] = pd.to_numeric(self.df['cantidad'], errors='coerce')
        negative_count = (self.df['cantidad'] < 0).sum()
        self.df['cantidad'] = self.df['cantidad'].abs()
        self.stats['negatives_fixed'] = negative_count
        
        # Asegurar que precio_unitario sea numérico
        self.df['precio_unitario'] = pd.to_numeric(
            self.df['precio_unitario'], 
            errors='coerce'
        )
        
        # Recalcular total
        self.df['total'] = (
            self.df['cantidad'] * self.df['precio_unitario']
        ).round(2)
        
        logger.info(f"   Valores negativos corregidos: {self.stats['negatives_fixed']}")
        return self
    
    def handle_nulls(self) -> 'DataTransformer':
        """Maneja valores nulos"""
        
        # Contar nulls por columna
        null_counts = self.df.isnull().sum()
        total_nulls = null_counts.sum()
        
        logger.debug("   Nulls por columna:")
        for col, count in null_counts[null_counts > 0].items():
            logger.debug(f"      {col}: {count}")
        
        # Rellenar nulls en campos opcionales
        self.df['vendedor'] = self.df['vendedor'].fillna('Sin asignar')
        self.df['cliente_id'] = self.df['cliente_id'].fillna('CLI-00000')
        
        self.stats['nulls_fixed'] = total_nulls
        logger.info(f"   Valores nulos tratados: {self.stats['nulls_fixed']}")
        
        return self
    
    def remove_duplicates(self) -> 'DataTransformer':
        """Elimina registros duplicados"""
        
        original_count = len(self.df)
        
        # Definir columnas para identificar duplicados
        dup_columns = [
            'fecha', 'producto', 'cantidad', 
            'precio_unitario', 'cliente_id'
        ]
        
        self.df = self.df.drop_duplicates(subset=dup_columns, keep='first')
        
        self.stats['duplicates_removed'] = original_count - len(self.df)
        logger.info(f"   Duplicados eliminados: {self.stats['duplicates_removed']}")
        
        return self
    
    def validate_and_reject(self) -> 'DataTransformer':
        """
        Valida registros y separa los inválidos.
        Los registros rechazados se almacenan para auditoría.
        """
        
        # Definir condiciones de validación
        valid_mask = (
            self.df['fecha'].notna() &
            self.df['cantidad'].notna() &
            (self.df['cantidad'] > 0) &
            self.df['precio_unitario'].notna() &
            (self.df['precio_unitario'] > 0) &
            self.df['producto'].notna() &
            (self.df['producto'] != '')
        )
        
        # Separar rechazados
        rejected_df = self.df[~valid_mask].copy()
        self.df = self.df[valid_mask].copy()
        
        # Almacenar razones de rechazo
        for idx, row in rejected_df.iterrows():
            reasons = []
            if pd.isna(row['fecha']):
                reasons.append('fecha_nula')
            if pd.isna(row['cantidad']) or row['cantidad'] <= 0:
                reasons.append('cantidad_invalida')
            if pd.isna(row['precio_unitario']) or row['precio_unitario'] <= 0:
                reasons.append('precio_invalido')
            if pd.isna(row['producto']) or row['producto'] == '':
                reasons.append('producto_vacio')
            
            self.rejected_records.append({
                'raw_data': row.to_json(),
                'rejection_reason': ', '.join(reasons)
            })
        
        logger.info(f"   Registros rechazados: {len(self.rejected_records)}")
        
        return self
    
    def add_metadata(self) -> 'DataTransformer':
        """Añade columnas de metadatos"""
        
        # Convertir fecha a formato estándar
        self.df['fecha'] = pd.to_datetime(self.df['fecha']).dt.date
        
        logger.info("   Metadatos añadidos")
        return self
    
    def get_result(self) -> Tuple[pd.DataFrame, List[dict], dict]:
        """Retorna DataFrame transformado, rechazados y estadísticas"""
        return self.df, self.rejected_records, self.stats


def transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[dict], dict]:
    """
    TRANSFORM: Aplica transformaciones y limpieza
    
    Args:
        df: DataFrame con datos raw
        
    Returns:
        Tuple con (DataFrame limpio, registros rechazados, estadísticas)
    """
    logger.info("=" * 60)
    logger.info("FASE: TRANSFORM")
    logger.info("=" * 60)
    
    transformer = DataTransformer(df)
    
    # Aplicar transformaciones en secuencia
    transformer \
        .clean_strings() \
        .fix_dates() \
        .fix_numeric_values() \
        .handle_nulls() \
        .remove_duplicates() \
        .validate_and_reject() \
        .add_metadata()
    
    df_clean, rejected, stats = transformer.get_result()
    
    # Resumen de transformación
    logger.info("-" * 40)
    logger.info("Resumen de Transformación:")
    logger.info(f"   Registros originales: {stats['original_count']:,}")
    logger.info(f"   Registros finales: {len(df_clean):,}")
    logger.info(f"   Tasa de rechazo: {len(rejected)/stats['original_count']*100:.2f}%")
    
    return df_clean, rejected, stats


# ============================================================
# FASE LOAD
# ============================================================

def create_tables(conn: MySQLConnection) -> bool:
    """Crea las tablas necesarias si no existen"""
    
    logger.info("Verificando/creando tablas...")
    
    # Leer script SQL
    sql_path = os.path.join(etl_config.sql_dir, 'create_tables.sql')
    
    if os.path.exists(sql_path):
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Ejecutar cada statement
        for statement in sql_script.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    conn.execute(statement)
                except MySQLError as e:
                    # Ignorar ciertos errores esperados
                    if 'already exists' not in str(e).lower():
                        logger.warning(f"SQL Warning: {e}")
        
        conn.commit()
        logger.info("   Tablas verificadas/creadas")
        return True
    
    else:
        # Crear tabla principal manualmente si no hay script
        create_ventas = """
        CREATE TABLE IF NOT EXISTS ventas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fecha DATE NOT NULL,
            producto VARCHAR(100) NOT NULL,
            categoria VARCHAR(50) NOT NULL,
            cantidad INT NOT NULL,
            precio_unitario DECIMAL(10, 2) NOT NULL,
            total DECIMAL(12, 2) NOT NULL,
            cliente_id VARCHAR(20),
            region VARCHAR(50) NOT NULL,
            vendedor VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(create_ventas)
        conn.commit()
        logger.info("   Tabla ventas creada")
        return True


def load(
    df: pd.DataFrame, 
    rejected: List[dict],
    execution_id: str
) -> Tuple[int, int]:
    """
    LOAD: Carga datos a MySQL
    
    Args:
        df: DataFrame con datos limpios
        rejected: Lista de registros rechazados
        execution_id: ID único de ejecución
        
    Returns:
        Tuple con (registros insertados, registros rechazados)
    """
    logger.info("=" * 60)
    logger.info("FASE: LOAD")
    logger.info("=" * 60)
    
    with get_mysql_connection() as conn:
        
        # Crear tablas
        create_tables(conn)
        
        # Truncar tabla de ventas (carga full refresh)
        logger.info("Truncando tabla ventas...")
        conn.execute("TRUNCATE TABLE ventas")
        conn.commit()
        
        # Preparar INSERT
        insert_query = """
        INSERT INTO ventas 
        (fecha, producto, categoria, cantidad, precio_unitario, 
         total, cliente_id, region, vendedor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convertir DataFrame a lista de tuplas
        records = df[[
            'fecha', 'producto', 'categoria', 'cantidad', 
            'precio_unitario', 'total', 'cliente_id', 
            'region', 'vendedor'
        ]].values.tolist()
        
        # Insertar en batches
        batch_size = etl_config.batch_size
        total_inserted = 0
        
        logger.info(f"Insertando {len(records):,} registros (batch size: {batch_size})...")
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                conn.executemany(insert_query, batch)
                conn.commit()
                total_inserted += len(batch)
                
                # Mostrar progreso
                progress = (i + len(batch)) / len(records) * 100
                logger.debug(f"   Progreso: {progress:.1f}% ({total_inserted:,} registros)")
                
            except MySQLError as e:
                logger.error(f"Error en batch {i//batch_size}: {e}")
                conn.rollback()
                raise
        
        logger.info(f"✅ Registros insertados: {total_inserted:,}")
        
        # Insertar registros rechazados (si hay tabla de rejected)
        if rejected:
            try:
                reject_query = """
                INSERT INTO ventas_rejected 
                (execution_id, raw_data, rejection_reason)
                VALUES (%s, %s, %s)
                """
                reject_records = [
                    (execution_id, r['raw_data'], r['rejection_reason']) 
                    for r in rejected
                ]
                conn.executemany(reject_query, reject_records)
                conn.commit()
                logger.info(f"   Rechazados registrados: {len(rejected)}")
            except MySQLError as e:
                logger.warning(f"No se pudieron guardar rechazados: {e}")
        
        # Verificar carga
        conn.execute("SELECT COUNT(*) as count FROM ventas")
        result = conn.cursor.fetchone()
        logger.info(f"   Total en tabla ventas: {result['count']:,}")
        
        return total_inserted, len(rejected)


# ============================================================
# ORQUESTADOR PRINCIPAL
# ============================================================

def run_etl() -> dict:
    """
    Ejecuta el pipeline ETL completo.
    
    Returns:
        Diccionario con métricas de ejecución
    """
    execution_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    
    logger.info("*" * 60)
    logger.info("INICIO DEL PIPELINE ETL")
    logger.info(f"Execution ID: {execution_id}")
    logger.info(f"Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("*" * 60)
    
    metrics = {
        'execution_id': execution_id,
        'status': 'STARTED',
        'start_time': start_time,
        'stages': {}
    }
    
    try:
        # EXTRACT
        extract_start = datetime.now()
        df_raw = extract(etl_config.csv_path)
        metrics['stages']['extract'] = {
            'records': len(df_raw),
            'duration': (datetime.now() - extract_start).total_seconds()
        }
        
        # TRANSFORM
        transform_start = datetime.now()
        df_clean, rejected, transform_stats = transform(df_raw)
        metrics['stages']['transform'] = {
            'records_in': transform_stats['original_count'],
            'records_out': len(df_clean),
            'rejected': len(rejected),
            'duration': (datetime.now() - transform_start).total_seconds()
        }
        
        # LOAD
        load_start = datetime.now()
        inserted, rejected_count = load(df_clean, rejected, execution_id)
        metrics['stages']['load'] = {
            'records_inserted': inserted,
            'duration': (datetime.now() - load_start).total_seconds()
        }
        
        # Éxito
        metrics['status'] = 'COMPLETED'
        
    except Exception as e:
        logger.error(f"❌ ERROR EN ETL: {e}")
        metrics['status'] = 'FAILED'
        metrics['error'] = str(e)
        raise
    
    finally:
        # Calcular duración total
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        metrics['end_time'] = end_time
        metrics['duration_seconds'] = duration
        
        # Resumen final
        logger.info("*" * 60)
        logger.info("RESUMEN DE EJECUCIÓN")
        logger.info("*" * 60)
        logger.info(f"Estado: {metrics['status']}")
        logger.info(f"Duración total: {duration:.2f} segundos")
        
        if 'stages' in metrics:
            for stage, data in metrics['stages'].items():
                logger.info(f"   {stage.upper()}: {data.get('duration', 0):.2f}s")
        
        logger.info("*" * 60)
    
    return metrics


# ============================================================
# PUNTO DE ENTRADA
# ============================================================

if __name__ == "__main__":
    try:
        # Verificar que existe el archivo de datos
        if not os.path.exists(etl_config.csv_path):
            logger.warning(f"Archivo no encontrado: {etl_config.csv_path}")
            logger.info("Ejecuta primero: python generate_synthetic_data.py")
            sys.exit(1)
        
        # Ejecutar ETL
        result = run_etl()
        
        if result['status'] == 'COMPLETED':
            logger.info("✅ ETL completado exitosamente")
            sys.exit(0)
        else:
            logger.error("❌ ETL falló")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("ETL cancelado por el usuario")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Error fatal: {e}")
        sys.exit(1)
