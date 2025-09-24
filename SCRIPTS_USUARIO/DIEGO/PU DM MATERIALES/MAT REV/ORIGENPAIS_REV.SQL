----------------------------------------------------------------------------------ORIGENPAIS_REV---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Validar la extracción de datos desde SAP y confirmar la cantidad de registros extraídos.  

SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_1CL_AR_P001;

SELECT * FROM RAW.SQ1_EXT_1CL_AR_P001;

--Verificar la carga de datos en la capa RAW mediante Alteryx.

SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_1CL_AR_P001;

--Confirmar la ejecución de las Stored Procedures (SPs) para transformar los datos de RAW a PRE y de PRE a CON.
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE TABLA_DESTINO IN ('PRE.PDIM_MAT_ORIGENPAIS_REV','CON.DIM_MAT_ORIGENPAIS_REV') ;

SELECT COUNT(*) FROM PRE.PDIM_MAT_ORIGENPAIS_REV;
SELECT COUNT(*) FROM CON.DIM_MAT_ORIGENPAIS_REV;

--Validar los datos en Snowflake comparándolos con los datos originales de SAP para asegurar la integridad.
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_1CL_AR_P001 UNION ALL
SELECT COUNT(*) FROM PRE.PDIM_MAT_ORIGENPAIS_REV UNION ALL
SELECT COUNT(*) FROM CON.DIM_MAT_ORIGENPAIS_REV;

--Verificar que la vista creada en Snowflake (si aplica) refleja correctamente las transformaciones y cumple con las reglas de negocio establecidas.

-- NO APLICA

--Validar las claves primarias y foráneas en la dimensión.
-- Ver definición de la tablas


--Verificar que no existan valores nulos o duplicados en campos clave.

SELECT ORIGENPAIS_REV_ID,  COUNT(*)
FROM PRE.PDIM_MAT_ORIGENPAIS_REV
GROUP BY ORIGENPAIS_REV_ID
HAVING COUNT(*) > 1;

SELECT ORIGENPAIS_REV_ID,  COUNT(*)
FROM CON.DIM_MAT_ORIGENPAIS_REV
GROUP BY ORIGENPAIS_REV_ID
HAVING COUNT(*) > 1;

SELECT *
FROM PRE.PDIM_MAT_ORIGENPAIS_REV
WHERE ORIGENPAIS_REV_ID IS NULL;

SELECT *
FROM CON.DIM_MAT_ORIGENPAIS_REV
WHERE ORIGENPAIS_REV_ID IS NULL;

--Validar que los tipos de datos y formatos de las columnas son consistentes con el modelo de datos maestro.

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'PDIM_MAT_ORIGENPAIS_REV' AND TABLE_SCHEMA = 'PRE';

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DIM_MAT_ORIGENPAIS_REV' AND TABLE_SCHEMA = 'CON';

--Confirmar que las transformaciones de campos entre capas cumplen con las reglas de negocio establecidas.
-- ver transformaciones en los SPs, tabla
SELECT * FROM PRE.PDIM_MAT_ORIGENPAIS_REV;
SELECT * FROM CON.DIM_MAT_ORIGENPAIS_REV;

--Validar que las fechas están dentro del rango esperado y que los parámetros de fechas se aplican correctamente.
--no hay fecha


--Verificar que las nomenclaturas de tablas y columnas siguen los estándares establecidos.

SELECT * FROM RAW.SQ1_EXT_1CL_AR_P001;
SELECT * FROM PRE.PDIM_MAT_ORIGENPAIS_REV;
SELECT * FROM CON.DIM_MAT_ORIGENPAIS_REV;

--Validar la estructura de la macro en Alteryx para asegurar que sigue las mejores prácticas y es eficiente.
-- ver el macro