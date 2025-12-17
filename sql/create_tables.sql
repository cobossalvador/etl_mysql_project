-- =============================================
-- Script DDL: Creación de tablas para ETL
-- Base de datos: etl_ventas
-- =============================================

-- Usar la base de datos
USE etl_ventas;

-- Eliminar tabla si existe (para recreación limpia)
DROP TABLE IF EXISTS ventas;

-- Crear tabla principal de ventas
CREATE TABLE ventas (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Índices para optimizar consultas
    INDEX idx_fecha (fecha),
    INDEX idx_producto (producto),
    INDEX idx_categoria (categoria),
    INDEX idx_region (region),
    INDEX idx_cliente (cliente_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Tabla de log para auditoría del ETL
DROP TABLE IF EXISTS etl_log;

CREATE TABLE etl_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    stage VARCHAR(50) NOT NULL,  -- 'EXTRACT', 'TRANSFORM', 'LOAD'
    status VARCHAR(20) NOT NULL, -- 'STARTED', 'COMPLETED', 'FAILED'
    records_processed INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    duration_seconds DECIMAL(10, 3),
    
    INDEX idx_execution (execution_id),
    INDEX idx_stage (stage),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Tabla para registros rechazados
DROP TABLE IF EXISTS ventas_rejected;

CREATE TABLE ventas_rejected (
    id INT AUTO_INCREMENT PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    raw_data TEXT NOT NULL,
    rejection_reason VARCHAR(255) NOT NULL,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_execution (execution_id),
    INDEX idx_reason (rejection_reason)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Vista para resumen de ventas por categoría
CREATE OR REPLACE VIEW v_ventas_por_categoria AS
SELECT 
    categoria,
    COUNT(*) as total_transacciones,
    SUM(cantidad) as unidades_vendidas,
    SUM(total) as total_ventas,
    AVG(total) as ticket_promedio,
    MIN(fecha) as primera_venta,
    MAX(fecha) as ultima_venta
FROM ventas
GROUP BY categoria
ORDER BY total_ventas DESC;


-- Vista para resumen de ventas por región
CREATE OR REPLACE VIEW v_ventas_por_region AS
SELECT 
    region,
    COUNT(*) as total_transacciones,
    SUM(cantidad) as unidades_vendidas,
    SUM(total) as total_ventas,
    AVG(total) as ticket_promedio,
    COUNT(DISTINCT cliente_id) as clientes_unicos
FROM ventas
GROUP BY region
ORDER BY total_ventas DESC;


-- Vista para ventas mensuales
CREATE OR REPLACE VIEW v_ventas_mensuales AS
SELECT 
    YEAR(fecha) as anio,
    MONTH(fecha) as mes,
    DATE_FORMAT(fecha, '%Y-%m') as periodo,
    COUNT(*) as total_transacciones,
    SUM(total) as total_ventas,
    AVG(total) as ticket_promedio
FROM ventas
GROUP BY YEAR(fecha), MONTH(fecha), DATE_FORMAT(fecha, '%Y-%m')
ORDER BY anio DESC, mes DESC;


-- Procedimiento para limpiar datos antiguos (retención de 2 años)
DELIMITER //

CREATE PROCEDURE sp_limpiar_datos_antiguos()
BEGIN
    DECLARE cutoff_date DATE;
    SET cutoff_date = DATE_SUB(CURDATE(), INTERVAL 2 YEAR);
    
    DELETE FROM ventas WHERE fecha < cutoff_date;
    
    SELECT CONCAT('Registros eliminados anteriores a: ', cutoff_date) as mensaje;
END //

DELIMITER ;


-- Mensaje de confirmación
SELECT 'Tablas y objetos creados exitosamente' as Status;
