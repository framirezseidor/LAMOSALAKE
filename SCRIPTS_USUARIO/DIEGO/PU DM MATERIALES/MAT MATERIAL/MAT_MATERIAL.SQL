SELECT * FROM LOGS.HISTORIAL_EJECUCIONES;
------------------------------------------------------------------------------------------MATERIAL--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Validar la extracción de datos desde SAP y confirmar la cantidad de registros extraídos.

SELECT COUNT(*) FROM RAW.SQ1_EXT_0MATERIAL_ATTR;

SELECT COUNT(*) FROM RAW.SQ1_EXT_0MATERIAL_TEXT;

SELECT COUNT(*) FROM RAW.SQ1_EXT_1CL_OMAT001;

--Verificar la carga de datos en la capa RAW mediante Alteryx.

SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_0MATERIAL_ATTR  UNION ALL
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_0MATERIAL_TEXT UNION ALL
SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_1CL_OMAT001;

--Confirmar la ejecución de las Stored Procedures (SPs) para transformar los datos de RAW a PRE y de PRE a CON.
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE TABLA_DESTINO IN ('PRE.PDIM_MAT_MATERIAL','CON.DIM_MAT_MATERIAL') ;

SELECT COUNT(*) FROM PRE.PDIM_MAT_MATERIAL;
SELECT COUNT(*) FROM CON.DIM_MAT_MATERIAL;

--Validar los datos en Snowflake comparándolos con los datos originales de SAP para asegurar la integridad.
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_0MATERIAL_ATTR  UNION ALL
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_0MATERIAL_TEXT UNION ALL
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_1CL_OMAT001 UNION ALL
SELECT COUNT(*) FROM PRE.PDIM_MAT_MATERIAL UNION ALL
SELECT COUNT(*) FROM CON.DIM_MAT_MATERIAL;

--Verificar que la vista creada en Snowflake (si aplica) refleja correctamente las transformaciones y cumple con las reglas de negocio establecidas.

SELECT * FROM CON.VW_DIM_MAT_MATERIAL_ADH;
SELECT * FROM CON.VW_DIM_MAT_MATERIAL_REV;

SELECT COUNT(*) FROM CON.VW_DIM_MAT_MATERIAL_ADH;
SELECT COUNT(*) FROM CON.VW_DIM_MAT_MATERIAL_REV;

--Validar las claves primarias y foráneas en la dimensión.
-- Ver definición de la tablas


--Verificar que no existan valores nulos o duplicados en campos clave.

SELECT MATERIAL_ID,  COUNT(*)
FROM PRE.PDIM_MAT_MATERIAL
GROUP BY MATERIAL_ID
HAVING COUNT(*) > 1;

SELECT MATERIAL_ID,  COUNT(*)
FROM CON.DIM_MAT_MATERIAL
GROUP BY MATERIAL_ID
HAVING COUNT(*) > 1;

SELECT *
FROM PRE.PDIM_MAT_MATERIAL
WHERE MATERIAL_ID IS NULL;

SELECT *
FROM CON.DIM_MAT_MATERIAL
WHERE MATERIAL_ID IS NULL;

--Validar que los tipos de datos y formatos de las columnas son consistentes con el modelo de datos maestro.

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'PDIM_MAT_MATERIAL' AND TABLE_SCHEMA = 'PRE';

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DIM_MAT_MATERIAL' AND TABLE_SCHEMA = 'CON';

--Confirmar que las transformaciones de campos entre capas cumplen con las reglas de negocio establecidas.
-- ver transformaciones en la tabla


--Validar que las fechas están dentro del rango esperado y que los parámetros de fechas se aplican correctamente.
--no hay fecha


--Verificar que las nomenclaturas de tablas y columnas siguen los estándares establecidos.

SELECT * FROM RAW.SQ1_EXT_0MATERIAL_ATTR;
SELECT * FROM RAW.SQ1_EXT_0MATERIAL_TEXT;
SELECT * FROM RAW.SQ1_EXT_1CL_OMAT001;
SELECT * FROM PRE.PDIM_MAT_MATERIAL;
SELECT * FROM CON.DIM_MAT_MATERIAL;

SELECT * FROM CON.VW_DIM_MAT_MATERIAL_ADH;
SELECT * FROM CON.VW_DIM_MAT_MATERIAL_REV;

--Validar la estructura de la macro en Alteryx para asegurar que sigue las mejores prácticas y es eficiente.
-- ver el macro

---------------------------------------------------------------------------------- MATERIALCENTRO---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Validar la extracción de datos desde SAP y confirmar la cantidad de registros extraídos.

SELECT COUNT(*) FROM RAW.SQ1_EXT_0MAT_PLANT_ATTR;

SELECT COUNT(*) FROM RAW.SQ1_EXT_0MAT_PLANT_TEXT;

--Verificar la carga de datos en la capa RAW mediante Alteryx.

SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_0MAT_PLANT_ATTR UNION ALL

SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_0MAT_PLANT_TEXT;

--Confirmar la ejecución de las Stored Procedures (SPs) para transformar los datos de RAW a PRE y de PRE a CON.
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE TABLA_DESTINO IN ('PRE.PDIM_MAT_MATERIALCENTRO','CON.DIM_MAT_MATERIALCENTRO') ;

SELECT COUNT(*) FROM PRE.PDIM_MAT_MATERIALCENTRO;
SELECT COUNT(*) FROM CON.DIM_MAT_MATERIALCENTRO;


--Validar los datos en Snowflake comparándolos con los datos originales de SAP para asegurar la integridad.
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_0MAT_PLANT_ATTR union all
SELECT COUNT(*) AS total_registros
FROM RAW.SQ1_EXT_0MAT_PLANT_TEXT union all
SELECT COUNT(*) FROM PRE.PDIM_MAT_MATERIALCENTRO union all
SELECT COUNT(*) FROM CON.DIM_MAT_MATERIALCENTRO;


--Verificar que la vista creada en Snowflake (si aplica) refleja correctamente las transformaciones y cumple con las reglas de negocio establecidas.

SELECT * FROM CON.VW_DIM_MAT_MATERIALCENTRO;

SELECT COUNT(*) FROM CON.VW_DIM_MAT_MATERIALCENTRO;


--Validar las claves primarias y foráneas en la dimensión.
-- Ver definición de la tablas


--Verificar que no existan valores nulos o duplicados en campos clave.

SELECT MATERIALCENTRO_ID,  COUNT(*)
FROM PRE.PDIM_MAT_MATERIALCENTRO
GROUP BY MATERIALCENTRO_ID
HAVING COUNT(*) > 1;

SELECT MATERIALCENTRO_ID,  COUNT(*)
FROM CON.DIM_MAT_MATERIALCENTRO
GROUP BY MATERIALCENTRO_ID
HAVING COUNT(*) > 1;

SELECT *
FROM PRE.PDIM_MAT_MATERIALCENTRO
WHERE MATERIALCENTRO_ID IS NULL;

SELECT *
FROM CON.DIM_MAT_MATERIALCENTRO
WHERE MATERIALCENTRO_ID IS NULL;

--Validar que los tipos de datos y formatos de las columnas son consistentes con el modelo de datos maestro.

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'PDIM_MAT_MATERIALCENTRO' AND TABLE_SCHEMA = 'PRE';

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DIM_MAT_MATERIALCENTRO' AND TABLE_SCHEMA = 'CON';

--Confirmar que las transformaciones de campos entre capas cumplen con las reglas de negocio establecidas.
-- ver transformaciones en los SPs, tablas
SELECT * FROM PRE.PDIM_MAT_MATERIALCENTRO;
SELECT * FROM CON.DIM_MAT_MATERIALCENTRO;

--Validar que las fechas están dentro del rango esperado y que los parámetros de fechas se aplican correctamente.
--no hay fecha


--Verificar que las nomenclaturas de tablas y columnas siguen los estándares establecidos.

SELECT * FROM RAW.SQ1_EXT_0MAT_PLANT_ATTR;
SELECT * FROM RAW.SQ1_EXT_0MAT_PLANT_TEXT;
SELECT * FROM PRE.PDIM_MAT_MATERIALCENTRO;
SELECT * FROM CON.DIM_MAT_MATERIALCENTRO;

--Validar la estructura de la macro en Alteryx para asegurar que sigue las mejores prácticas y es eficiente.
-- ver el macro
SELECT * FROM CON.VW_DIM_MAT_MATERIALCENTRO;

---------------------------------------------------------------------------------- MATERIALVENTAS---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Validar la extracción de datos desde SAP y confirmar la cantidad de registros extraídos.

SELECT COUNT(*) FROM RAW.SQ1_EXT_0MAT_SALES_ATTR;

SELECT COUNT(*) FROM RAW.SQ1_EXT_0MAT_SALES_TEXT;



--Verificar la carga de datos en la capa RAW mediante Alteryx.

SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_0MAT_SALES_ATTR  UNION ALL

SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_0MAT_SALES_TEXT;

--Confirmar la ejecución de las Stored Procedures (SPs) para transformar los datos de RAW a PRE y de PRE a CON.
SELECT * FROM LOGS.HISTORIAL_EJECUCIONES WHERE TABLA_DESTINO IN ('PRE.PDIM_MAT_MATERIALVENTAS','CON.DIM_MAT_MATERIALVENTAS') ;

SELECT COUNT(*) FROM PRE.PDIM_MAT_MATERIALVENTAS;
SELECT COUNT(*) FROM CON.DIM_MAT_MATERIALVENTAS;

--Validar los datos en Snowflake comparándolos con los datos originales de SAP para asegurar la integridad.
SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_0MAT_SALES_ATTR UNION ALL
SELECT COUNT(*) AS total_registros_raw
FROM RAW.SQ1_EXT_0MAT_SALES_TEXT UNION ALL
SELECT COUNT(*) FROM PRE.PDIM_MAT_MATERIALVENTAS UNION ALL
SELECT COUNT(*) FROM CON.DIM_MAT_MATERIALVENTAS;

--Verificar que la vista creada en Snowflake (si aplica) refleja correctamente las transformaciones y cumple con las reglas de negocio establecidas.

SELECT * FROM CON.VW_DIM_MAT_MATERIALVENTAS;

SELECT COUNT(*) FROM CON.VW_DIM_MAT_MATERIALVENTAS;---181335


--Validar las claves primarias y foráneas en la dimensión.
-- Ver definición de la tablas


--Verificar que no existan valores nulos o duplicados en campos clave.

SELECT MATERIALVENTAS_ID, COUNT(*)
FROM PRE.PDIM_MAT_MATERIALVENTAS
GROUP BY MATERIALVENTAS_ID
HAVING COUNT(*) > 1;

SELECT MATERIALVENTAS_ID,  COUNT(*)
FROM CON.DIM_MAT_MATERIALVENTAS
GROUP BY MATERIALVENTAS_ID
HAVING COUNT(*) > 1;

SELECT *
FROM PRE.PDIM_MAT_MATERIALVENTAS
WHERE MATERIALVENTAS_ID IS NULL;

SELECT *
FROM CON.DIM_MAT_MATERIALVENTAS
WHERE MATERIALVENTAS_ID IS NULL;

--Validar que los tipos de datos y formatos de las columnas son consistentes con el modelo de datos maestro.

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'PDIM_MAT_MATERIALVENTAS' AND TABLE_SCHEMA = 'PRE';

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DIM_MAT_MATERIALVENTAS' AND TABLE_SCHEMA = 'CON';

--Confirmar que las transformaciones de campos entre capas cumplen con las reglas de negocio establecidas.
-- ver transformaciones en los SPs, tabla
SELECT * FROM PRE.PDIM_MAT_MATERIALVENTAS;
SELECT * FROM CON.DIM_MAT_MATERIALVENTAS;

--Validar que las fechas están dentro del rango esperado y que los parámetros de fechas se aplican correctamente.
--no hay fecha


--Verificar que las nomenclaturas de tablas y columnas siguen los estándares establecidos.

SELECT * FROM RAW.SQ1_EXT_0MAT_SALES_ATTR;
SELECT * FROM RAW.SQ1_EXT_0MAT_SALES_TEXT;
SELECT * FROM PRE.PDIM_MAT_MATERIALVENTAS;
SELECT * FROM CON.DIM_MAT_MATERIALVENTAS;

--Validar la estructura de la macro en Alteryx para asegurar que sigue las mejores prácticas y es eficiente.
-- ver el macro

SELECT * FROM CON.VW_DIM_MAT_MATERIALVENTAS;