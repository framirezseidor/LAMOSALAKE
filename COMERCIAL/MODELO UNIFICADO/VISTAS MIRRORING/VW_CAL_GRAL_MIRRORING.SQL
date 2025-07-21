CREATE OR REPLACE VIEW MIRRORING.VW_CAL_GRAL_MIRRORING
AS
    WITH CTE_FECHAS AS (
    SELECT DATEADD(DAY, SEQ4(), '2010-01-01') AS FECHA
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
    )
    SELECT
    -- Fecha sin hora
    CAST(FECHA AS DATE) AS FECHA,

    -- Año, Mes, Día
    YEAR(FECHA) AS ANIO,
    MONTH(FECHA) AS MES,
    DAY(FECHA) AS DIA,

    -- AÑO + MES en formato YYYY-MM
    TO_CHAR(FECHA, 'YYYY-MM') AS ANIOMES,

    -- Día de la semana reducido a 3 letras en español
    CASE DAYOFWEEK(FECHA)
        WHEN 0 THEN 'Dom'
        WHEN 1 THEN 'Lun'
        WHEN 2 THEN 'Mar'
        WHEN 3 THEN 'Mie'
        WHEN 4 THEN 'Jue'
        WHEN 5 THEN 'Vie'
        WHEN 6 THEN 'Sab'
    END AS DIA_NOMBRE,

    -- Nombre del mes reducido a 3 letras en español
    CASE MONTH(FECHA)
        WHEN 1 THEN 'Ene' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dic'
    END AS MES_NOMBRE,

    -- Trimestre (Q1-Q4)
    'Q' || QUARTER(FECHA) AS TRIMESTRE,

    -- Número de semana del año
    WEEKOFYEAR(FECHA) AS SEMANA_NUMERO,

    -- Fecha en texto: DD Mes Abrev Año
    TO_CHAR(DAY(FECHA), '00') || ' ' || 
    CASE MONTH(FECHA)
        WHEN 1 THEN 'Ene' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dic'
    END || ' ' || TO_CHAR(YEAR(FECHA)) AS FECHA_TEXTO

    FROM CTE_FECHAS

END;


