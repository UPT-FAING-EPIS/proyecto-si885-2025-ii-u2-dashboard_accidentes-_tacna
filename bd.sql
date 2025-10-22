-- =============================================
-- Base de datos: incidentes
-- Descripción: Almacena noticias sobre incidentes y accidentes en Tacna
-- Motor: Azure SQL Server (T-SQL)
-- =============================================

-- Si necesitas crear la base de datos (solo para testing local)
-- En Azure SQL, la base de datos ya está creada por Terraform
-- CREATE DATABASE incidentes;
-- GO

-- Usar la base de datos
USE incidentestacna;
GO

-- =============================================
-- Tabla: medios
-- Información de medios informativos locales
-- =============================================
IF OBJECT_ID('medios', 'U') IS NOT NULL
    DROP TABLE medios;
GO

CREATE TABLE medios (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nombre NVARCHAR(150) NOT NULL,
    tipo NVARCHAR(100) NOT NULL,  -- Tipo: radio, digital, televisión, prensa
    region NVARCHAR(100) NOT NULL,
    url_principal NVARCHAR(300) NULL
);
GO

-- =============================================
-- Tabla: noticias
-- Noticias extraídas de medios locales
-- =============================================
IF OBJECT_ID('noticias', 'U') IS NOT NULL
    DROP TABLE noticias;
GO

CREATE TABLE noticias (
    id INT IDENTITY(1,1) PRIMARY KEY,
    titulo NVARCHAR(500) NOT NULL,
    url NVARCHAR(500) NOT NULL,
    fuente NVARCHAR(150) NOT NULL,  -- Medio que publicó
    categoria NVARCHAR(100) NULL,    -- Actualidad, Policial, Tránsito
    fecha_publicacion DATE NOT NULL,
    fecha_extraccion DATE NOT NULL,
    ciudad NVARCHAR(100) NOT NULL DEFAULT 'Tacna',
    sentimiento NVARCHAR(50) NULL,   -- positivo, negativo, neutro
    
    -- Índices para mejorar rendimiento de consultas
    INDEX IX_noticias_fecha_publicacion (fecha_publicacion),
    INDEX IX_noticias_fuente (fuente),
    INDEX IX_noticias_categoria (categoria)
);
GO

-- =============================================
-- Tabla: accidentes
-- Detalles estructurados de accidentes reportados
-- =============================================
IF OBJECT_ID('accidentes', 'U') IS NOT NULL
    DROP TABLE accidentes;
GO

CREATE TABLE accidentes (
    id INT IDENTITY(1,1) PRIMARY KEY,
    noticia_id INT NOT NULL,
    fecha_accidente DATE NULL,
    hora_aproximada NVARCHAR(50) NULL,        -- mañana, tarde, noche, madrugada
    dia_semana NVARCHAR(20) NULL,
    distrito NVARCHAR(100) NULL,
    tipo_via NVARCHAR(100) NULL,              -- avenida, carretera, calle
    tipo_accidente NVARCHAR(100) NULL,        -- choque, atropello, volcadura
    causa_probable NVARCHAR(200) NULL,
    num_vehiculos_involucrados INT NULL,
    tipo_vehiculo NVARCHAR(100) NULL,         -- auto, moto, bus
    condicion_clima NVARCHAR(100) NULL,
    num_fallecidos INT NOT NULL DEFAULT 0,
    num_heridos INT NOT NULL DEFAULT 0,
    danos_materiales NVARCHAR(50) NULL,       -- leve, moderado, grave
    intervencion_bomberos BIT NOT NULL DEFAULT 0,  -- 1 = sí, 0 = no
    coordenadas NVARCHAR(100) NULL,           -- latitud,longitud
    tiempo_publicacion INT NULL,              -- días entre accidente y publicación
    
    -- Foreign Key
    CONSTRAINT FK_accidentes_noticias FOREIGN KEY (noticia_id) 
        REFERENCES noticias(id) ON DELETE CASCADE,
    
    -- Índices
    INDEX IX_accidentes_fecha (fecha_accidente),
    INDEX IX_accidentes_tipo (tipo_accidente),
    INDEX IX_accidentes_distrito (distrito)
);
GO

-- =============================================
-- Tabla: keywords
-- Palabras clave extraídas para análisis de texto
-- =============================================
IF OBJECT_ID('keywords', 'U') IS NOT NULL
    DROP TABLE keywords;
GO

CREATE TABLE keywords (
    id INT IDENTITY(1,1) PRIMARY KEY,
    noticia_id INT NOT NULL,
    palabra_clave NVARCHAR(100) NOT NULL,
    
    -- Foreign Key
    CONSTRAINT FK_keywords_noticias FOREIGN KEY (noticia_id) 
        REFERENCES noticias(id) ON DELETE CASCADE,
    
    -- Índice
    INDEX IX_keywords_palabra (palabra_clave)
);
GO

-- =============================================
-- Datos de ejemplo (OPCIONAL - comentar si no se necesita)
-- =============================================


-- =============================================
-- Vistas útiles para análisis
-- =============================================

-- Vista: Resumen de accidentes por distrito
CREATE OR ALTER VIEW vw_accidentes_por_distrito AS
SELECT 
    distrito,
    COUNT(*) as total_accidentes,
    SUM(num_fallecidos) as total_fallecidos,
    SUM(num_heridos) as total_heridos,
    COUNT(CASE WHEN intervencion_bomberos = 1 THEN 1 END) as con_intervencion
FROM accidentes
WHERE distrito IS NOT NULL
GROUP BY distrito;
GO

-- Vista: Noticias con detalles de accidente
CREATE OR ALTER VIEW vw_noticias_accidentes AS
SELECT 
    n.id,
    n.titulo,
    n.fuente,
    n.fecha_publicacion,
    n.categoria,
    a.fecha_accidente,
    a.tipo_accidente,
    a.distrito,
    a.num_fallecidos,
    a.num_heridos,
    a.causa_probable
FROM noticias n
LEFT JOIN accidentes a ON n.id = a.noticia_id;
GO

-- Vista: Estadísticas por tipo de accidente
CREATE OR ALTER VIEW vw_estadisticas_tipo_accidente AS
SELECT 
    tipo_accidente,
    COUNT(*) as cantidad,
    SUM(num_fallecidos) as total_fallecidos,
    SUM(num_heridos) as total_heridos,
    AVG(num_vehiculos_involucrados) as promedio_vehiculos
FROM accidentes
WHERE tipo_accidente IS NOT NULL
GROUP BY tipo_accidente;
GO

-- =============================================
-- Procedimientos almacenados útiles
-- =============================================

-- Procedimiento: Buscar noticias por palabra clave
CREATE OR ALTER PROCEDURE sp_buscar_por_keyword
    @keyword NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT DISTINCT
        n.id,
        n.titulo,
        n.fuente,
        n.fecha_publicacion,
        n.url
    FROM noticias n
    INNER JOIN keywords k ON n.id = k.noticia_id
    WHERE k.palabra_clave LIKE '%' + @keyword + '%'
    ORDER BY n.fecha_publicacion DESC;
END;
GO

-- Procedimiento: Obtener resumen del mes
CREATE OR ALTER PROCEDURE sp_resumen_mensual
    @anio INT,
    @mes INT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        COUNT(DISTINCT n.id) as total_noticias,
        COUNT(DISTINCT a.id) as total_accidentes,
        SUM(a.num_fallecidos) as total_fallecidos,
        SUM(a.num_heridos) as total_heridos,
        COUNT(DISTINCT n.fuente) as medios_activos
    FROM noticias n
    LEFT JOIN accidentes a ON n.id = a.noticia_id
    WHERE YEAR(n.fecha_publicacion) = @anio 
      AND MONTH(n.fecha_publicacion) = @mes;
END;
GO

PRINT 'Base de datos "incidentestacna" creada exitosamente';
PRINT 'Tablas creadas: medios, noticias, accidentes, keywords';
PRINT 'Vistas creadas: vw_accidentes_por_distrito, vw_noticias_accidentes, vw_estadisticas_tipo_accidente';
PRINT 'Procedimientos: sp_buscar_por_keyword, sp_resumen_mensual';
GO